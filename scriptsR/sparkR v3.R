#
### Install Packages
#

install.packages("Rcpp", repos="https://rcppcore.github.io/drat")
install.packages('sparklyr', repos = "http://cran.us.r-project.org")
install.packages("googleCloudStorageR", repos = "http://cran.us.r-project.org")
install.packages("jsonlite", repos = "http://cran.us.r-project.org")
args <- commandArgs(trailing = TRUE)

#
### Get from Cloud Storage
#

library(googleCloudStorageR)
gcs_auth("/tmp/tart-12b0c03bcce1.json")
gcs_global_bucket("tart-90ca2.appspot.com")
objects <- gcs_list_objects()

filePath <- args[[1]]
if (filePath %in% objects$name) {
  filePosition <- which(filePath == objects$name)
}
initialDataNoRewrite <- gcs_get_object(objects$name[[filePosition]])

#
### Load Spark
#

library("SparkR")
sparkR.session(appName = "cloudrun-r-sparkr")
library(sparklyr)
sparklyr::spark_install(version = "2.3.0", reset = TRUE)

for (slide in initialDataNoRewrite) {
  if (identical(slide$delimiter, "mySQL")) {
    config <- spark_config()
    config$`sparklyr.shell.driver-class-path` <- "/tmp/mysql-connector-java-8.0.20.jar"
    sc <- spark_connect(master = "yarn-client", config = config)
    break
  }
}

if (!exists("sc")) {
  sc <- spark_connect(master = "yarn-client")
}


#
### Load Tables to Spark
#

for (slide in initialDataNoRewrite) {
  # if input, load the input to apache Spark with sheet names ie. Sheet1, Sheet2

  if (identical(slide$type, "input")) {
    if (identical(slide$delimiter, "mySQL")) {
      eval(call("<-", as.name(gsub(" ", "_", slide$name)), 
        spark_read_jdbc(sc, name = gsub(" ", "_", slide$name), options = list(
          url = slide$login$url,
          user = slide$login$user,
          password = slide$login$password,
          dbtable = slide$login$dbtable
        ))
      ))
    } else {
      pathToFileSpark <- paste(args[[2]], slide$file, sep = "")
      eval(call(
        "<-", as.name(gsub(" ", "_", slide$name)), 
        spark_read_csv(sc, 
          name = gsub(" ", "_", slide$name), 
          path = pathToFileSpark, 
          header = TRUE, 
          delimiter = slide$delimiter
        )
      ))
    }
  }
}

#
## Function Required for Computation
#

listToDataframe <- function(data) {
  df <- as.data.frame(matrix(NA, (length(data) - 1), (length(data[[1]]) - 1) ))
  for ( row in 1:(length(data)-1) ) {
     for ( column in 1:(length(data[[1]]) - 1) ) {
       if ( !is.null(data[[row+1]][[column+1]]$value) ) {
         df[row,column] <- (data[[row+1]][[column+1]]$value)
         }
     }
  }
    for ( column in 1:(length(data[[1]])-1) ) {
      tryCatch({
      colnames(df)[column] <- data[[1]][[column+1]]
      }, error=function(e){})
    }
  return(df)
}

rawSheetNamesNoRewrite <- character(length(initialDataNoRewrite))
if (length(initialDataNoRewrite) > 0) {
  for (i in 1:length(initialDataNoRewrite)) rawSheetNamesNoRewrite[i] <- initialDataNoRewrite[[i]]$name
}

sheetNamesNoRewrite <- character(length(initialDataNoRewrite))
if (length(initialDataNoRewrite) > 0) {
  # for (i in 1:length(initialDataNoRewrite)) sheetNamesNoRewrite[i] <- initialDataNoRewrite[[i]]$name
  for (i in 1:length(initialDataNoRewrite)) sheetNamesNoRewrite[i] <- gsub(" ", "_", initialDataNoRewrite[[i]]$name)
}

translateRForProccess <- function(cell, currentSlide) {
  if (is.null(cell) || is.na(cell) || !identical(substring(cell,1,1),"=")) return(cell)
  match <- regmatches(cell, gregexpr("[[:upper:]]+\\d+", cell))[[1]]

  removedEqualsCell <- gsub("'", "`", substring(cell, 2), fixed = TRUE)
  for (i in 1:length(initialDataNoRewrite)) removedEqualsCell <- gsub(rawSheetNamesNoRewrite[i], sheetNamesNoRewrite[i], removedEqualsCell)
  if (identical(match, character(0))) return(removedEqualsCell)
  
  LETTERS_REFERENCE <- letters[1:26]
  for (reference in 1:length(match)) {
    column <-  match(tolower(regmatches(match[reference], regexpr("[[:upper:]]{,2}", match[reference]))), LETTERS_REFERENCE)-1
    row <- as.numeric(regmatches(match[reference], regexpr("\\d+", match[reference])))-1
    
    if (!identical(reference+1,length(match)-1)) {
      column2 <- match(tolower(regmatches(match[reference+1], regexpr("[[:upper:]]{,2}", match[reference+1]))), LETTERS_REFERENCE)
      row2 <- as.numeric(regmatches(match[reference], regexpr("\\d+", match[reference])))+1
      
      ref4 <- paste("[", row, ":", row2, ",", column, ":", column2, "]", sep = "")
      prefix4 <- paste("!", match[reference], ":", match[reference+1], sep = "")
      removedEqualsCell <- gsub(prefix4, ref4,removedEqualsCell)
      
      ref3 <- paste("`", sheetNamesNoRewrite[currentSlide], "`", "[", row, ":", row2, ",", column, ":", column2, "]", sep = "")
      prefix3 <- paste(match[reference], ":", match[reference+1], sep = "")
      removedEqualsCell <- gsub(prefix3, ref3,removedEqualsCell)
    }
    ref <- paste("[", row, ",", column, "]", sep = "")
    prefix <- paste("!", match[reference], sep = "")
    removedEqualsCell <- gsub(prefix, ref,removedEqualsCell)
    
    ref2 <- paste("`", sheetNamesNoRewrite[currentSlide], "`", "[", row, ",", column, "]", sep = "")
    prefix2 <- match[reference]
    removedEqualsCell <- gsub(prefix2, ref2,removedEqualsCell)
  }
  return(removedEqualsCell)
}

evaluateCellNoRewrite <- function(cell) {
  if (is.null(cell) || is.na(cell) || identical(cell, "")) return(cell)
    
  result <- cell
  try(
    while(!identical(result,eval(parse(text = result)))) {
      if (is.na(eval(parse(text = result))) || length(eval(parse(text = result))) > 1) break
      result <- eval(parse(text = result))
    }, silent = TRUE
  )
  
  if(is.null(result) || is.function(result)) return(cell)
  return(result) 
}

computeSparklyR <- function(cell) {
  # find out which Sheet is used in the cell
  if (is.null(cell) || is.na(cell) || identical(cell, "")) return(cell)
  cell <- evaluateCellNoRewrite(cell)
  if (is.numeric(cell)) return(cell)
  
  for (name in sheetNamesNoRewrite) {
    proto_name <- regmatches(cell, regexpr(name, cell))
    proto_cell <- gsub(name, "e", cell)
  }
  if (identical(proto_name, character(0))) return(cell)
  
  function_text <- paste("`", proto_name, "`", "%>% spark_apply(function(e)", proto_cell, ")", sep = "")
  pickup_dropoff_tbl <- tryCatch({
    eval(parse(text = function_text))
  }, error = function(cond) return('#ERROR!'))
  
  if (identical(pickup_dropoff_tbl, '#ERROR!')) return(pickup_dropoff_tbl)
                                 
  pickup_dropoff <- collect(pickup_dropoff_tbl)
  if (length(pickup_dropoff[[1]]) > 1) return(cell)
  return(pickup_dropoff[[1]])
}

splitAndReplace <- function(cell) {
  if (is.null(cell) || is.na(cell) || identical(cell, "") || is.numeric(cell)) return(cell)
  stringParts <- strsplit(cell, "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\(|\\)")[[1]]
  evaluateParts <- simplify2array(lapply(stringParts, computeSparklyR))
  
  newCell <- cell
  for (part in 1:length(stringParts)) {
    if(!identical(stringParts[part],"")) {
      newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed = TRUE)
    }
  }
  
  if (grepl('#ERROR!', newCell)) return('#ERROR!')
  newCell <- evaluateCellNoRewrite(newCell)
  if (is.numeric(newCell)) return(newCell)

  stringParts <- strsplit(newCell, "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%")[[1]]
  evaluateParts <- simplify2array(lapply(stringParts, computeSparklyR))
  
  for (part in 1:length(stringParts)) {
    if(!identical(stringParts[part],"")) {
      newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed = TRUE)
    }
  }
  return(evaluateCellNoRewrite(newCell))
}

saveBackToJsonFromDataFrame <- function(dataframe, json) {
  for (row in 1:(length(json)-1) ) {
    for (column in 1:(length(json[[1]])-1) ) {
      if (!is.null(json[[row+1]][[column+1]]$value) && startsWith(json[[row+1]][[column+1]]$value, "=")) {
        json[[row+1]][[column+1]]$value <- toString(dataframe[row,column][[1]])
      }
    }
  }
  return(json)
}

#
## Compute Spark
#

for (slide in 1:length(initialDataNoRewrite)) {
  if (identical(initialDataNoRewrite[[slide]]$type, "sheet")) {
    longitudeTemporaryData <- listToDataframe(initialDataNoRewrite[[slide]]$data)
    longitudeTemporaryData <- apply(longitudeTemporaryData, 1:2, translateRForProccess)
    eval(call("<-", as.name(initialDataNoRewrite[[slide]]$name), longitudeTemporaryData))
  }
}

for (slide in 1:length(initialDataNoRewrite)) {
  if (identical(initialDataNoRewrite[[slide]]$type, "sheet")) {
    longitudeTemporaryData <- apply(eval(parse(text = initialDataNoRewrite[[slide]]$name)), 1:2, splitAndReplace)
    initialDataNoRewrite[[slide]]$data <- saveBackToJsonFromDataFrame(longitudeTemporaryData, initialDataNoRewrite[[slide]]$data)
  }
}

#
# Upload back to cloud storage
#

json_structure <- function(input, output) {
  jsonlite::write_json(input, output, pretty = FALSE, auto_unbox = TRUE, null = c("null"))
}

substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}
runCounter <- as.numeric(substrRight(max(grep(args[[1]], objects$name, value=TRUE)), 1))
if (is.na(runCounter)) {
  runCounter <- 1
} else {
  runCounter <- runCounter + 1
}

if (grepl("run", args[[1]])) {
  newPathToFileGCP <- paste(args[[1]], runCounter, sep = " ")
} else {
  newPathToFileGCP <- paste(args[[1]], "run", runCounter, sep = " ")
}

gcs_upload(initialDataNoRewrite, name=newPathToFileGCP, object_function = json_structure)

sparkR.session.stop()
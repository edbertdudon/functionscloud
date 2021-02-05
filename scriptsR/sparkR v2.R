### Arguments
# 1. filePath
# 2. user folder path

#
### Install Packages and Load Spark
#

install.packages("Rcpp", repos="https://rcppcore.github.io/drat")
install.packages('sparklyr', repos = "http://cran.us.r-project.org")
# install.packages("dplyr", repos = "http://cran.us.r-project.org")
install.packages("googleCloudStorageR", repos = "http://cran.us.r-project.org")
install.packages("jsonlite", repos = "http://cran.us.r-project.org")

args <- commandArgs(trailing = TRUE)
library("SparkR")
sparkR.session(appName = "cloudrun-r-sparkr")
library(sparklyr)
sparklyr::spark_install(version = "2.3.0", reset = TRUE)
sc <- spark_connect(master = "yarn-client")

#
### Get from Cloud Storage
#

library(googleCloudStorageR)
gcs_auth("/tmp/tart-12b0c03bcce1.json")
gcs_global_bucket("tart-90ca2.appspot.com")
objects <- gcs_list_objects()

filePath <- args[[1]]
if (filePath %in% objects$name) {
  # filePosition <- which(grepl(filePath, objects$name))
  filePosition <- which(filePath == objects$name)
}
lattitude <- gcs_get_object(objects$name[[filePosition]])

#
### Load Tables to Spark
#

for (slide in lattitude) {
  print(slide$delimiter)
  # if input, load the input to apache Spark with sheet names ie. Sheet1, Sheet2
  if (slide$type == "input") {
    pathToFileSpark <- paste(args[[2]], slide$file, sep = "")
    eval(call("<-", as.name(slide$name), spark_read_csv(sc, name=slide$name, path=pathToFileSpark, header=TRUE, delimiter=slide$delimiter)))
  }
}

#
## Function Required for Computation
#

create_dataframe <- function(data) {
  df <- as.data.frame(matrix(NA, (length(data) - 1), (length(data[[1]]) - 1) ))
  for ( row in 1:(length(data)-1) ) {
     for ( column in 1:(length(data[[1]]) - 1) ) {
       if ( !is.null(data[[row+1]][[column+1]]) ) {
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

sheetNames <- character(length(lattitude))
if (length(lattitude) > 0) {
  for (i in 1:length(lattitude)) sheetNames[i] <- lattitude[[i]]$name
}

Compute_SparklyR <- function(cell) {
  if (!startsWith(cell, "=")) {
    return(cell)
  }

  nu_cell <- substring(cell, 2)

  # find out which Sheet is used in the cell
  count <- 0
  for (name in sheetNames) {
    if (grepl(name, nu_cell)) {
      count <- count + 1
      proto_name <- name
      proto_cell <- gsub(name, "e", nu_cell)
    }
  }
  if (count > 1) {
    return(cell)
  } else {
    if (exists("proto_name")) {
      function_text <- paste(proto_name, "%>% spark_apply(function(e)", proto_cell, ")")
      pickup_dropoff_tbl <- eval(parse(text = function_text))
      pickup_dropoff <- collect(pickup_dropoff_tbl)
      return(pickup_dropoff[[1]])
    } 
    else {
      pickup_dropoff <- tryCatch({
        eval(parse(text = nu_cell))
      },
      error=function(cond) {
        return(cell)
      })
      return(pickup_dropoff)
    }
  }
}

save_json <- function(dataframe, json) {
  for (row in 1:(length(json)-1) ) {
    for (column in 1:(length(json[[1]])-1) ) {
      if (!is.null(json[[row+1]][[column+1]])) {
        json[[row+1]][[column+1]]$value <- toString(dataframe[row,column][[1]])
      }
    }
  }
  return(json)
}

#
## Compute Spark
#

longitude <- lattitude
for (slide in 1:length(longitude)) {  
  # run each sheet and return the sheets to form the new file
  if (longitude[[slide]]$type == "sheet") {
    # transit_slide <- apply(create_dataframe(longitude[[slide]]$data), 1:2, function(cell) eval(parse(text = Compute_SparklyR(cell))) )          
    transit_slide <- apply(create_dataframe(longitude[[slide]]$data), 1:2, Compute_SparklyR)
    # collect results
    longitude[[slide]]$data <- save_json(transit_slide, longitude[[slide]]$data)
  }
}

#
# Upload back to cloud storage
#

json_structure <- function(input, output) {
  jsonlite::write_json(input, output, pretty = FALSE, auto_unbox = TRUE, null = c("null"))
}

pathToFileGCP <- paste(args[[1]], "run", sep = " ")

runCounter <- length(which(grepl(args[[1]], objects$name)))

newPathToFileGCP <- paste(pathToFileGCP, runCounter, sep = " ")
gcs_upload(longitude, name=newPathToFileGCP, object_function = json_structure)

sparkR.session.stop()
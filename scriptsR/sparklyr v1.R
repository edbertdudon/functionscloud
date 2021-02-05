install.packages("Rcpp")
if (!require("sparklyr")) {
  install.packages("sparklyr")  
}
install.packages("dplyr")

library(sparklyr)
# library(dplyr)

sc <- spark_connect(method = "databricks")

install.packages("googleCloudStorageR")

library(googleCloudStorageR)

gcs_auth("/dbfs/FileStore/tables/tart_12b0c03bcce1-5758d.json")
gcs_global_bucket("tart-90ca2.appspot.com")
objects <- gcs_list_objects()

# how to know what file to get
filePath <- dbutils.widgets.get('filepath')
# filePath <- "user/x9J4UnfNyDTfcPu4FilasMZdb1K3/Untitled Worksheet 1.json"
if (filePath %in% objects$name) {
  filePosition <- which(grepl(filePath, objects$name))
}
lattitude <- gcs_get_object(objects$name[[filePosition]])

# lattitude <- gcs_get_object(objects$name[[3]])

install.packages("readr")
library(readr)

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

# Load tables to Spark
for (slide in lattitude) {
  # if input, load the input to apache Spark with sheet names ie. Sheet1, Sheet2
  if (slide$type == "input") {
    df <- format_delim( create_dataframe(slide$data), " ", na="NA", append = FALSE, col_names = TRUE, quote_escape = "double")
    pathToFileStore <- paste("/FileStore/store/", slide$name, sep = "")
    dbutils.fs.put(pathToFileStore, df, overwrite=TRUE)

    pathToFileSpark <- paste("file:///dbfs/FileStore/store/", slide$name, sep = "")
#     slide$name <- spark_read_csv(sc, name=slide$name, path=pathToFileSpark, header=TRUE, delimiter=" ")
#     slide$name <- copy_to(sc, create_dataframe(slide$data))
    eval(call("<-", as.name(slide$name), spark_read_csv(sc, name=slide$name, path=pathToFileSpark, header=TRUE, delimiter=" ")))
  }
}

# run computations in Spark

# sheet names vector
names <- character(length(lattitude))
if (length(lattitude) > 0) {
  for (i in 1:length(lattitude)) names[i] <- lattitude[[i]]$name
}

Compute_SparklyR <- function(cell) {
  # find out which Sheet is used in the cell
  count <- 0
  for (name in names) {
    if (grepl(name, cell)) {
      count <- count + 1
      proto_name <- name
      proto_cell <- gsub(name, "e", cell)
    }
  }
  if (count > 1) {
    return(cell)
  } else {
    if (exists("proto_name")) {
      function_text <- paste(proto_name, "%>% spark_apply(function(e)", proto_cell, ")")
      pickup_dropoff_tbl <- eval(parse(text = function_text))
      pickup_dropoff <- collect(pickup_dropoff_tbl)
      return(pickup_dropoff[1,1])
    } 
    else {
      pickup_dropoff <- tryCatch({
        eval(parse(text = cell))
      },
      error=function(cond) {
        return(cell)
      })
      return(pickup_dropoff)
    }
  }
}

save_json <- function(dataframe, json) {
#   for ( column in 1:(length(json[[1]])-1) ) {
#     colnames(dataframe)[column] <- "value"
#     json[[1]][[column]] <- colnames(dataframe)[column]
#   }
  
  for (row in 1:(length(json)-1) ) {
    for (column in 1:(length(json[[1]])-1) ) {
      if (!is.null(json[[row+1]][[column+1]])) {
        json[[row+1]][[column+1]]$value <- toString(lapply(transit_slide,"[",column)[[row]])
      }
    }
  }
  return(json)
}

for (slide in 1:length(lattitude)) {  
  # run each sheet and return the sheets to form the new file
  if (lattitude[[slide]]$type == "sheet") {
    transit_slide <- apply(create_dataframe(lattitude[[slide]]$data), 1:2, 
                            function(cell) tryCatch({ 
                              eval(parse(text = Compute_SparklyR(cell) ))
                            }, error=function(cond) {return(cell)}) 
                          )
                           
    # collect results
    lattitude[[slide]]$data <- save_json(transit_slide, lattitude[[slide]]$data)
  }
}

json_structure <- function(input, output) {
  jsonlite::write_json(input, output, pretty = FALSE, auto_unbox = TRUE, null = c("null"))
}

attempt <- 1
r <- NULL
while( is.null(r) && attempt <= 3 ) {
  attempt <- attempt + 1
  try(
#     r <- gcs_upload(lattitude, name="user/x9J4UnfNyDTfcPu4FilasMZdb1K3/Untitled Worksheet 1.json", object_function = json_structure) 
    r <- gcs_upload(lattitude, name=filePath, object_function = json_structure) 
  )
} 
# dbutils.notebook.exit()

# Notes

# Local environement
# Sys.setenv(JAVA_HOME = "/Library/Java/JavaVirtualMachines/jdk1.8.0_73.jdk/Contents/Home")
# Sys.setenv(SPARK_HOME = "/usr/local/Cellar/apache-spark/2.4.4")

# Case that works "mean(Sheet1$Rating_X) + mean(Sheet1$Rating_Y)"
# Case that doesn't work "mean(Sheet1$Rating_X) + mean(Sheet2$Rating_Y)"

# this works
# Sheet1 %>% spark_apply(function(e) eval(parse(text="mean(e$Rating_X)")) )
# test <- eval(parse(text="Sheet1 %>% spark_apply(function(e) mean(e$Rating_X))"))
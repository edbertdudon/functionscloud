#
#  App.R
#
#  Created by Edbert Dudon on 7/8/19.
#  Copyright © 2019 Project Tart. All rights reserved.
#
# Tutorial link: https://ericjinks.com/blog/2019/serverless-R-cloud-run/
#
# Deploy Steps gcloud:
# 1. cd rscript-R
# 2. gcloud builds submit --tag gcr.io/tart-90ca2/cloudrun-r-rscript
# 3. gcloud run deploy --image gcr.io/tart-90ca2/cloudrun-r-rscript --platform managed
#
# Deploy Steps Local:
# docker build . -t 'cloudrun-r-rscript'
# docker run -p 8080:8080 -e PORT=8080 cloudrun-r-rscript
#

library(jsonlite)
library(ggplot2)
library(dplyr)
library(broom)

#' @filter cors
cors <- function(req, res) {
  # res$setHeader("Access-Control-Allow-Origin", "https://www.tartcl.com")
  res$setHeader("Access-Control-Allow-Origin", "http://localhost:3000")

  if (req$REQUEST_METHOD == "OPTIONS") {
    # res$setHeader("Access-Control-Allow-Methods","https://www.tartcl.com")
    res$setHeader("Access-Control-Allow-Methods","http://localhost:3000")
    res$setHeader("Access-Control-Allow-Headers", req$HTTP_ACCESS_CONTROL_REQUEST_HEADERS)
    res$status <- 200
    return(list())
  } else {
    plumber::forward()
  }

}

#' parse cell
#' @param slides worksheet data
#' @param cell the cell we want to parse
#' @param names array of sheet names
#' @post /cloudR
function(slides, cell, names) {
  lattitude <- fromJSON(slides)
  sheetNames <- fromJSON(names)

  evaluateCell <- function(cell) {
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

  splitAndReplace <- function(cell) {
    if (is.null(cell) || is.na(cell) || identical(cell, "") || is.numeric(cell)) return(cell)

    stringParts <- strsplit(cell, "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\(|\\)")[[1]]
    evaluateParts <- simplify2array(lapply(stringParts, evaluateCell))
    newCell <- cell
    for (part in 1:length(stringParts)) {
      if(!identical(stringParts[part],"")) {
        newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed = TRUE)
      }
    }
    return(evaluateCell(newCell))
  }
  
  for (slide in 1:length(lattitude)) {
    # Configure sheets in matrix
    longitude <- matrix(unlist(lattitude[[slide]]), ncol=length(lattitude[[slide]][[1]]$value), byrow=T)
    colnames(longitude) <- longitude[1,]
    longitude <- longitude[-1,]
    eval(call("<-", as.name(sheetNames[slide]), longitude))
  }
  
  # "Sheet1!C3 -- Sheet1!C4 -- Dec-31 or 0.42 or 1+3"
  # "Sheet1!C3 -- Sheet1!C4 + Sheet1!C5 -- 0.43"
  storedString <- splitAndReplace(evaluateCell(cell))
  if (is.numeric(storedString)) {
    jsonString <- toJSON(storedString)
  } else {
    # "Sheet1!C3 + Sheet1!C6 -- 42 or Dec-31+0.1919"
    # "Sheet1!C3 -- Dec-31"
    for (slide in 1:length(sheetNames)) {
      # "Sheet1!C3 -- mean(Sheet1$Rating_X) + 0.43"
      if (grepl("$", storedString, fixed=TRUE)) {
        eval(call(
          "<-", as.name(sheetNames[slide]), 
          data.frame(apply(
            eval(parse(text = paste("`",sheetNames[slide],"`", sep = "")))
          , 1:2, function(x) as.numeric(as.character(x))), stringsAsFactors = FALSE)
        ))
      # "Sheet1!C3 -- mean(Sheet1[,2:3]) + 0.43"
      } else {
        eval(call(
          "<-", as.name(sheetNames[slide]), 
          apply(eval(parse(text = paste("`",sheetNames[slide],"`", sep = ""))), 1:2, function(x) as.numeric(as.character(x)))
        ))
      }
      
      newStoredString <- splitAndReplace(storedString)
      if (identical(newStoredString, storedString)) {
        jsonString <- toJSON('#ERROR!')
      } else {
        jsonString <- toJSON(newStoredString)
      }
    }
  }
  
  jsonString
}

#' Plot out data
#' @param slides worksheet data
#' @param name current sheet name
#' @param names array of sheet names
#' @param type chart type
#' @param variablex X Variable
#' @param variabley Y Variable
#' @post /plot
#' @png
function(slides, name, names, type, variablex, variabley) {
  lattitude <- fromJSON(slides)
  sheetNamesNoRewrite <- fromJSON(names)

  tryNumeric <- function(cell) {
      newCell <- as.numeric(as.character(cell))
      if (is.na(newCell)) {
          return(cell)
      } else {
          return(newCell)
      }
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
  
  computeStandardR <- function(cell) {
    # find out which Sheet is used in the cell
    if (is.null(cell) || is.na(cell) || identical(cell, "")) return(cell)
    cell <- evaluateCellNoRewrite(cell)
    if (is.numeric(cell)) return(cell)

    for (sheetname in sheetNamesNoRewrite) {
      proto_name <- regmatches(cell, regexpr(sheetname, cell))
      proto_cell <- gsub(sheetname, "longitudeTemporaryTable", cell)
    }

    if (length(proto_name) == 0) return(cell)
    longitudeTemporaryTable <- eval(parse(text = paste("`", proto_name, "`", sep="")))
    longitudeTemporaryTable <<- data.frame(apply(longitudeTemporaryTable, 1:2, function(x) as.numeric(as.character(x))), stringsAsFactors=FALSE)
    # longitudeTemporaryTable <<- data.frame(lapply(longitudeTemporaryTable, tryNumeric), stringsAsFactors = FALSE)
    
    pickup_dropoff <- tryCatch({
      evaluateCellNoRewrite(proto_cell)
    }, error = function(cond) return('#ERROR!'))
    
    if (grepl("longitudeTemporaryTable", pickup_dropoff)) return(cell)
    if (identical(pickup_dropoff, '#ERROR!')) return(pickup_dropoff)
    return(pickup_dropoff)
  }
  
  splitAndReplaceNew <- function(cell) {
    if (is.null(cell) || is.na(cell) || identical(cell, "") || is.numeric(cell)) return(cell)

    stringParts <- strsplit(cell, "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\(|\\)")[[1]]
    evaluateParts <- simplify2array(lapply(stringParts, computeStandardR))
    
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
    evaluateParts <- simplify2array(lapply(stringParts, computeStandardR))
    
    for (part in 1:length(stringParts)) {
      if(!identical(stringParts[part],"")) {
        newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed = TRUE)
      }
    }
    return(evaluateCellNoRewrite(newCell))
  }

  for (slide in 1:length(lattitude)) {
    # Configure sheets in matrix
    longitude <- matrix(unlist(lattitude[[slide]]), ncol=length(lattitude[[slide]][[1]]$value), byrow=T)
    colnames(longitude) <- longitude[1,]
    longitude <- longitude[-1,-1]
    eval(call("<-", as.name(sheetNamesNoRewrite[slide]), longitude))
  }
  
  currentLattitude <- apply(eval(parse(text = paste("`", name, "`", sep = ""))), 1:2, splitAndReplaceNew)
  currentLattitude <- as.data.frame(lapply(currentLattitude, tryNumeric), stringsAsFactors = FALSE)
  
  if (hasArg(variabley)) {
    pngChart <- ggplot(currentLattitude, aes_string(variablex, variabley)) + eval(parse(text = type))
  } else {
    pngChart <-ggplot(currentLattitude, aes_string(variablex)) + eval(parse(text = type))
  }

  print(pngChart)
}

#' Analyze data
#' @param slides worksheet data
#' @param names array of sheet names
#' @param formula formula
#' @param name current sheet name
#' @post /regression
function(slides, names, formula, name) {
  lattitude <- fromJSON(slides)
  sheetNamesNoRewrite <- fromJSON(names)
  
  tryNumeric <- function(cell) {
    newCell <- as.numeric(as.character(cell))
    if (is.na(newCell)) {
      return(cell)
    } else {
      return(newCell)
    }
  }
  
  evaluateCell <- function(cell) {
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
  
  splitAndReplace <- function(cell) {
    if (is.null(cell) || is.na(cell) || identical(cell, "") || is.numeric(cell)) return(cell)
    
    stringParts <- strsplit(cell, "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\(|\\)")[[1]]
    evaluateParts <- simplify2array(lapply(stringParts, evaluateCell))
    newCell <- cell
    for (part in 1:length(stringParts)) {
      if(!identical(stringParts[part],"")) {
        newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed = TRUE)
      }
    }
    return(evaluateCell(newCell))
  }
  
  evaluateCellNoRewrite <- function(cell, actualData, sheetname) {
    if (is.null(cell) || is.na(cell) || identical(cell, "")) return(cell)
    
    eval(call("<-", as.name(sheetname), actualData))
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
  
  splitAndReplaceNoRewrite <- function(cell, actualData, sheetname) {
    if (is.null(cell) || is.na(cell) || identical(cell, "") || is.numeric(cell)) return(cell)

    stringParts <- strsplit(cell, "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\(|\\)")[[1]]
    evaluateParts <- simplify2array(lapply(stringParts, evaluateCellNoRewrite, actualData=actualData, sheetname=sheetname))

    newCell <- cell
    for (part in 1:length(stringParts)) {
      if(!identical(stringParts[part],"")) {
        newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed = TRUE)
      }
    }
    print(evaluateCellNoRewrite(newCell, actualData=actualData, sheetname=sheetname))
    return(evaluateCellNoRewrite(newCell, actualData=actualData, sheetname=sheetname))
  }
  
  computeStandardR <- function(cell) {
    # find out which Sheet is used in the cell
    if (is.null(cell) || is.na(cell) || identical(cell, "")) return(cell)
    storedString <- splitAndReplace(evaluateCell(cell))
    if (is.numeric(storedString)) return(storedString)

    for (slide in 1:length(sheetNamesNoRewrite)) {
      # "Sheet1!C3 -- mean(Sheet1$Rating_X) + 0.43"
      if (grepl("$", storedString, fixed=TRUE)) {
        eval(call(
          "<-", as.name(sheetNamesNoRewrite[slide]),
          data.frame(apply(
            eval(parse(text = paste("`",sheetNamesNoRewrite[slide],"`", sep = "")))
            , 1:2, function(x) as.numeric(as.character(x))), stringsAsFactors = FALSE)
        ))
        # "Sheet1!C3 -- mean(Sheet1[,2:3]) + 0.43"
      } else {
        eval(call(
          "<-", as.name(sheetNamesNoRewrite[slide]),
          apply(eval(parse(text = paste("`",sheetNamesNoRewrite[slide],"`", sep = ""))), 1:2, function(x) as.numeric(as.character(x)))
        ))
      }
      
      newStoredString <- splitAndReplaceNoRewrite(storedString, eval(parse(text = paste("`",sheetNamesNoRewrite[slide],"`", sep = ""))), sheetNamesNoRewrite[slide])
      if (!identical(newStoredString, storedString)) {
        return(newStoredString)
      }
    }
  }
  
  for (slide in 1:length(lattitude)) {
    # Configure sheets in matrix
    longitude <- matrix(unlist(lattitude[[slide]]), ncol=length(lattitude[[slide]][[1]]$value), byrow=T)
    colnames(longitude) <- longitude[1,]
    longitude <- longitude[-1,]
    eval(call("<-", as.name(sheetNamesNoRewrite[slide]), longitude))
  }

  currentLattitude <- data.frame(apply(eval(parse(text = paste("`", name, "`", sep = ""))), 1:2, computeStandardR), stringsAsFactors = FALSE)
  print(currentLattitude)
  currentLattitude <- data.frame(lapply(currentLattitude, tryNumeric), stringsAsFactors = FALSE)

  regressionParsed <- eval(parse(text = paste(formula, ", currentLattitude) %>% tidy()", sep="")))
  jsonString <- toJSON(regressionParsed)

  jsonString
}
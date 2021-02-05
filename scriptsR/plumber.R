#
# plumber.R
#
# Created by Edbert Dudon on 7/8/19.
# Copyright © 2019 Project Tart. All rights reserved.
#
# Commands:
# plumb(file='/Users/eb/bac/functionscloud/scriptsR/plumber.R')$run()
#
library(plumber)
library(jsonlite)
library(slam)
library(ggplot2)
library(dplyr)
library(broom)
library(pastecs)
library(car)
library(rrcov)
library(ROI)
library(ROI.plugin.glpk)
library(ROI.plugin.qpoases)
library(ROI.plugin.optimx)
library(ROI.plugin.lpsolve)
library(ROI.plugin.quadprog)
library(ROI.plugin.nloptr)
library(ROI.plugin.ecos)

options(pillar.sigfigs=100)

#* @filter cors
cors <- function(res) {
  res$setHeader('Access-Control-Allow-Origin', '*')
  res$setHeader('Access-Control-Allow-Headers', 'Content-Type')
  res$setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
  plumber::forward()
}

configureSheetsToMatrix <- function() {
  for (s in 1:length(slidesNoRewrite)) {
    longitude <- unlist(slidesNoRewrite[s])
    totalcolumns <- length(slidesNoRewrite[s][[1]][[1]])
    longitude <- matrix(longitude, ncol=totalcolumns, byrow=T)
    # colnames(longitude) <- longitude[1,]
    # longitude <- longitude[-1,]
    eval(call('<<-', as.name(dataNamesNoRewrite[s]), longitude))
  }
}

notApplicable <- function(cell) {
  is.null(cell) || is.na(cell) || cell=='' || !is.character(cell)
}

evalParse <- function(cell) {
  eval(parse(text=cell))
}

FULL_COLUMN_ROW <- '\\[{1},{1}\\d+\\:?\\d*\\]{1}|\\[{1}\\d+\\:?\\d*\\,{1}\\]{1}'

evaluateCell <- function(cell) {
  if (notApplicable(cell)) return(cell)
  result <- cell
  try(
    # cell != NA returns NA instead of TRUE/FALSE
    while(!identical(result, evalParse(result))) {
      # mean(test.csv[,1:1]) not mean(sheet1[,1:1]) (cell referenced: 'sheet1[2:2]')
      if (grepl(FULL_COLUMN_ROW, result)
        && gregexpr('(', result, fixed=T)[[1]] != -1
        && length(gregexpr('(', result, fixed=T)[[1]]) == 1
        && gregexpr(')', result, fixed=T)[[1]] != -1
        && length(gregexpr(')', result, fixed=T)[[1]])) {
        nextResult <- tryCatch({
          resultNaRm <- substr(result, 1, nchar(result)-1)
          resultNaRm <- paste0(resultNaRm, ',na.rm=T)')
          resultNaRm <- evalParse(resultNaRm)
          if (!(is.numeric(resultNaRm) && length(resultNaRm) == 1)) {
            # cbind(Intercept=1,fibrinogen=test.csv[,1:1]
            # If other columns are longer, will take in the longer length. Leads to trailing Intercept 1's.
            resultNaRm <- evalParse(result)
          }
          resultNaRm
        }, error = function(cond) NA)
      } else {
        nextResult <- evalParse(result)
      }
      # simple_triplet_matrix(1,1,1)
      if (is.simple_triplet_matrix(nextResult)) {
        result <- as.matrix(nextResult)
        break
      }
      if (is.na(nextResult)) break
      result <- nextResult
      # break length(result) > 1 after because we want its results
      if (length(result) > 1) break
    }
  )
  # Use silent for cleaner code
  # ,silent=T)
  if(is.null(result) || is.function(result)) return(cell)
  return(result)
}

EQUATION_FORM_SPLIT <- '\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|(?!\\d),(?!\\d)|\\s|(?<=\\])\\s*\\,|\\,(?=\\()|(?<=\\))\\,|\\((?=\\()|(?<=\\))\\)'

splitAndReplace <- function(cell, dataname) {
  # matrix(c(1:9), 3), sheet1[1:10,1] for length(cell) > 1
  cell <- evaluateCell(cell)
  if (notApplicable(cell) || is.numeric(cell) || length(cell) > 1) return(cell)
  if (is.factor(cell)) {
    return(as.numeric(cell))
  }
  stringParts <- strsplit(cell, EQUATION_FORM_SPLIT, perl=T)[[1]]
  # sapply used to distinguish with and without quotes simultaneously 'sum(mean(test.csv[,2:2], na.rm=T),`test.csv`[3,3])'
  if (!is.null(dataname)) {
    stringParts <- unlist(sapply(stringParts, function(x) {
      datanameWithQuote <- paste0('`', dataname, '`')
      if (grepl(datanameWithQuote, x)) {
        dataNameRegex <- paste0('\\({1}(?!', datanameWithQuote, ')')
      } else {
        dataNameRegex <- paste0('\\({1}(?!', dataname, ')')
      }
      return(strsplit(x, dataNameRegex, perl=T)[[1]])
      # if (!grepl(FULL_COLUMN_ROW, x) && grepl('\\(|\\)', x)) {
      #   if (!is.null(dataname)) {
      #     dataNameRegex <- paste0(dataNameRegex, '|\\(|\\)')
      #   } else {
      #     dataNameRegex <- '\\(|\\)'
      #   }
      #   return(strsplit(x, dataNameRegex, perl=T)[[1]])
      # } else {
      #   return(x)
      # }
    }, USE.NAMES=F))
  }
  evaluateParts <- sapply(stringParts, evaluateCell, USE.NAMES=F)
  # Some cases of matrix multiplication %*% produces list resulting in evaluateCell working when we don't want it to
  if (identical(typeof(evaluateParts), 'list')) {
    evaluateParts <- stringParts
  }
  newCell <- cell
  for (part in 1:length(stringParts)) {
    if (!identical(stringParts[part],'')) {
      newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed=T)
    }
  }
  evaluateCell(newCell)
}

hasCellReference <- function(cell) {
  for (sheet in dataNamesNoRewrite) {
    if (grepl(sheet, cell)) {
      return(T)
    }
  }
  return(F)
}

applyNumeric <- function(slide) {
  slide <- evalParse(paste0('`', slide, '`'))
  apply(slide, 1:2, function(x) as.numeric(as.character(x)))
  # na.omit(slide)
}

numerizeAndSplit <- function(cell, original) {
  if (notApplicable(cell)) return(cell)
  # 'Sheet1!C3 -- Sheet1!C4 -- Dec-31 or 0.42 or 1+3'
  # 'Sheet1!C3 -- Sheet1!C4 + Sheet1!C5 -- 0.43'
  if (is.numeric(cell) || !hasCellReference(cell)) {
    return(cell)
  }
  # 'Sheet1!C3 + Sheet1!C6 -- 42 or Dec-31+0.1919'
  # 'Sheet1!C3 -- Dec-31'
  for (slide in 1:length(dataNamesNoRewrite)) {
    currentSlide <- dataNamesNoRewrite[slide]
    name <- as.name(currentSlide)
    longitude <- applyNumeric(currentSlide)
    # 'Sheet1!C3 -- mean(Sheet1$Rating_X) + 0.43'
    if (grepl('$', cell, fixed=T)) {
      eval(call('<<-', name, as.data.frame(longitude, stringsAsFactors=F)))
    # 'Sheet1!C3 -- mean(Sheet1[,2:3]) + 0.43'
    } else {
      eval(call('<<-', name, longitude))
    }
    # Without cell reference: 'mean(Sheet1[2:3,2:2]) + mean(Sheet1[3:4,2:2])'
    # Without cell reference: 'Sheet4[1:3,1:3] %*% Sheet4[1:3,2:4]'
    # 'mean(Sheet4$Rating_X)' will not work.
    # No header cell reference. In order to accomodate Inputs alongside other cell reference.
    newStoredString <- splitAndReplace(cell, currentSlide)
    # Repeats when sheetNames ['test.csv', 'sheet1'] instead of ['sheet1', 'test.csv']
    if (!identical(newStoredString, cell)) return(newStoredString)
  }
  original
}

computeStandardR <- function(cell) {
  if (notApplicable(cell)) return(cell)
  # Matrix multiplication expected to be numeric so cell should not be evaluated prior to applyNumeric
  # Should not evaluateCell prior to splitAndReplace, only first element will be used
  storedString <- cell
  if (!grepl('%*%', cell)) {
    nextStoredString <- splitAndReplace(storedString, NULL)
    while(storedString != nextStoredString) {
      storedString <- nextStoredString
      # matrix(c(1:9), 3), sheet1[1:10,1] for length(cell) > 1
      if (grepl('%*%', storedString) || length(storedString) > 1) break
      nextStoredString <- splitAndReplace(storedString, NULL)
    }
  }
  numerizeAndSplit(storedString, cell)
}

configureAndCompute <- function(cell) {
  if (notApplicable(cell)) return(cell)
  configureSheetsToMatrix()
  computeStandardR(cell)
}

getErrorType <- function(cell) {
  for (sheet in dataNamesNoRewrite) {
    circularReference <- paste0('\\`?(?<=', sheet, ')\\`?\\[{1}\\d+\\,{1}\\d+\\]{1}')
    if (grepl(circularReference, cell, perl=T)) {
      return('#REF!')
    }
  }
  return('#ERROR!')
}

isError <- function(cell) {
  cell == '#ERROR!' || cell == '#REF!'
}

#' parse cell
#' @param slides worksheet data
#' @param cell the cell we want to parse
#' @param names array of sheet names
#' @post /cloudR
function(slides, cell, names) {
	slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
	dataNamesNoRewrite <<- fromJSON(names)
  result <- configureAndCompute(cell)
  # With cell reference: 'mean(Sheet1[2:3,2:2]) + mean(Sheet1[3:4,2:2]) -- mean(Sheet2[,2:2]), mean(Sheet2[,3:3])'
  # Matrix with cell reference: `sheet1`[1:3,1:3]%*%`sheet1`[1:3,1:3]
  if (result == cell) {
    configureSheetsToMatrix()
    for (name in dataNamesNoRewrite) {
      if (grepl(name, cell)) {
        currentLattitude <- evalParse(paste0('`', name, '`'))
        currentLattitude <- apply(currentLattitude, 1:2, function(x) configureAndCompute(x)[1])
        eval(call('<<-', as.name(name), currentLattitude))
      }
    }
    result <- computeStandardR(cell)
  }
  if (result == cell) {
    result <- getErrorType(result)
  # needed so that cells don't get rewritten when users don't want them to (e.g., cell referencing a matrix)
  } else if (length(result) > 1 && !grepl('%*%', cell)) {
    result <- result[1]
  }
  toJSON(result, digits=NA)
}

CELL_REGEX <- '\\[{1}\\d+\\,{1}\\d+\\]{1}'
RANGE_REGEX <- '\\[{1}\\d*\\:?\\d*\\,{1}\\d*\\:?\\d*\\]{1}'

# sheet1[1,], sheet1[,1:1], sheet1[1,2], sheet1[1:2,2], sheet1[1:2,1:2]
appendMatchNumbers <- function(range) {
  matchRange <- regmatches(range, regexpr(RANGE_REGEX, range))
  matchNumbers <- regmatches(matchRange, gregexpr('\\d+', matchRange))[[1]]
  if(length(matchNumbers) == 1 && grepl(FULL_COLUMN_ROW, matchRange)) {
    matchSlide <- sub(FULL_COLUMN_ROW, '', range)
    # sheet1[,2]
    if (grepl('\\[{1},{1}\\d+\\:?\\d*\\]{1}', range)) {
      rowLength <- nrow(evalParse(matchSlide))
      result <- c(1, rowLength, matchNumbers, matchNumbers)
    # sheet1[2,]
    } else {
      colLength <- ncol(evalParse(matchSlide))
      result <- c(matchNumbers, matchNumbers, 1, colLength)
    }
  } else if (length(matchNumbers) == 2 && grepl(CELL_REGEX, matchRange)) {
    # sheet[1,2]
    result <- c(matchNumbers[1], matchNumbers[1], matchNumbers[2], matchNumbers[2])
  } else if (length(matchNumbers) == 2 && grepl('\\d+\\:{1}\\d+\\,{1}|\\,{1}\\d+\\:{1}\\d+', matchRange)) {
    matchSlide <- sub(FULL_COLUMN_ROW, '', range)
    # sheet1[2:2,]
    if (grepl('\\d+\\:{1}\\d+\\,{1}', matchRange)) {
      colLength <- ncol(evalParse(matchSlide))
      result <- c(matchNumbers, 1, colLength)
    # sheet1[,2:2]
    } else {
      rowLength <- nrow(evalParse(matchSlide))
      result <- c(1, rowLength, matchNumbers)
    }
  } else if (length(matchNumbers) == 3 && grepl('\\d+\\:{1}\\d+\\,{1}\\d+|\\d+\\,{1}\\d+\\:{1}\\d+', matchRange)) {
    # sheet[1:2,2]
    if (grepl('\\d+\\:{1}\\d+\\,{1}\\d+', matchRange)) {
      result <- append(matchNumbers, matchNumbers[3], after=3)
    # sheet[1,1:2]
    } else {
      result <- append(matchNumbers, matchNumbers[1], after=0)
    }
  } else {
    result <- matchNumbers
  }
  return(as.numeric(result))
}

createLattitudeFromRange <- function(range) {
  configureSheetsToMatrix()
  currentSlide <- evaluateCell(range)
  if (is.vector(currentSlide)) {
    matchNumbers <- appendMatchNumbers(range)
    # sheet1[1,1:2]
    currentSlide <- matrix(currentSlide, ncol=matchNumbers[4]-matchNumbers[3]+1)
  }
  currentLattitude <- apply(currentSlide, 1:2, function(x) configureAndCompute(x)[1])
  # Check which sheet is used
  checklist <- dataNamesNoRewrite
  for (n in dataNamesNoRewrite) {
    if (any(grepl(n, currentLattitude))) {
      checklist <- checklist[checklist != n]
    }
  }
  checklist <- match(dataNamesNoRewrite, checklist)
  for (n in 1:length(checklist)) {
    if (is.na(checklist[n])) {
      # Previous sheets might be NA from configureAndCompute
      configureSheetsToMatrix()
      currentLattitude <- evalParse(paste0('`', dataNamesNoRewrite[n], '`'))
      currentLattitude <- apply(currentLattitude, 1:2, function(x) configureAndCompute(x)[1])
      eval(call('<<-', as.name(dataNamesNoRewrite[n]), currentLattitude))
    }
  }
  # Must splitAndReplace cells to level 1 cell reference. eval(call()) will replace sheets to numeric causing cell reference to return NA
  currentLattitude <- apply(currentSlide, 1:2, function(x) splitAndReplace(x, NULL)[1])
  apply(currentLattitude, 1:2, function(x) numerizeAndSplit(x, x)[1])
}

rangeToMatchSlideQuotes <- function(range) {
  matchSlide <- sub(RANGE_REGEX, '', range)
  if (!grepl('`', matchSlide)) {
    matchSlide <- paste0('`', matchSlide, '`')
  }
  matchSlide
}

lattitudeAsDataframe <- function(firstrow, currentLattitude, range) {
  if (firstrow == 'true') {
    if (ncol(currentLattitude) == 1) {
      cname <- currentLattitude[1,1]
      currentLattitude <- as.matrix(currentLattitude[-1,])
      colnames(currentLattitude) <- cname
    } else {
      colnames(currentLattitude) <- currentLattitude[1,]
      currentLattitude <- currentLattitude[-1,]
      if (is.vector(currentLattitude)) {
        cnames <- names(currentLattitude)
        currentLattitude <- matrix(currentLattitude, nrow=1)
        colnames(currentLattitude) <- cnames
      }
    }
  } else {
    if (length(appendMatchNumbers(range)) != 4) {
      stop('Invalid range.')
    }
    rowLength <- length(evalParse(rangeToMatchSlideQuotes(range))[,1])
    matchNumbers <- appendMatchNumbers(range)
    colLength <- matchNumbers[4]-matchNumbers[3]+1
    colnames <- vector(mode='character', length=colLength)
    for (i in 1:colLength) {
      letter <- columnToLetter(matchNumbers[3]+i-1)
      colnames[i] <- paste0(letter, matchNumbers[1], ':', letter, matchNumbers[2])
    }
    colnames(currentLattitude) <- colnames
  }
  currentLattitude <- apply(currentLattitude, 1:2, as.numeric)
  as.data.frame(currentLattitude)
}

#' Plot out data
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param types chart type
#' @param variablex X Variable
#' @param variabley Y Variable
#' @post /plot
#' @png
function(slides, names, range, firstrow, types, variablex, variabley) {
  if (variablex == 'na') {
    pngChart <- ggplot() + geom_blank()
  } else {
    slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
    dataNamesNoRewrite <<- fromJSON(names)
    # createLattitudeFromRange must be seperate or errors will occur
    currentLattitude <<- createLattitudeFromRange(range)
    currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  	# PLOT
    types <- paste0(fromJSON(types), collapse='+')
    variablex <- paste0('`',variablex,'`')
    if (hasArg(variabley)) {
      variabley <- paste0('`',variabley,'`')
      pngChart <- evalParse(paste0('ggplot(currentLattitude, aes(x=', variablex, ',y=', variabley, '))+', types))
    	# pngChart <- ggplot(currentLattitude, aes_string(paste0('`',variablex,'`'), paste0('`',variabley,'`'))) + types
    } else {
      pngChart <- evalParse(paste0('ggplot(currentLattitude, aes(x=', variablex, '))+', types))
    	# pngChart <- ggplot(currentLattitude, aes_string(paste0('`',variablex,'`'))) + types
    }
  }
  print(pngChart)
}
#
# #' Analyze data
# #' @param slides worksheet data
# #' @param names array of sheet names
# #' @param range current sheet range
# #' @param firstrow Should set first row as headers?
# #' @param formulatext formula
# #' @post /regression
# function(slides, names, range, firstrow, formulatext) {
#   slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
#   dataNamesNoRewrite <<- fromJSON(names)
#   currentLattitude <- createLattitudeFromRange(range)
#   currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
# 	# REGRESS
# 	tryCatch({
      # evalParse(formulatext)
#   )}, error = function(cond) {
#     return(toString(cond))
#   })
# }

jsonTidy <- function(variable) {
  toJSON(tidy(variable), digits=NA)
}

jsonRownamesDf <- function(variable) {
  toJSON(tibble::rownames_to_column(as.data.frame(variable)), digits=NA)
}

jsonTidyAov <- function(formula) {
  jsonTidy(aov(evalParse(formula), currentLattitude))
}

jsonTidyManova <- function(formula) {
  jsonTidy(manova(evalParse(formula), currentLattitude))
}

#' Descriptive Statistics
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablesx variablesx to use
#' @post /statdesc
function(slides, names, range, firstrow, variablesx) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  currentLattitude[fromJSON(variablesx)] %>%
    stat.desc() %>%
    jsonTidy
}

#' One-Way frequency table
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param columns columns to use
#' @post /ftable
function(slides, names, range, firstrow, columns) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  # ftable(currentLattitude[fromJSON(columns)])
}

#' Chi-squared test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @post /chisqtest
function(slides, names, range, firstrow, variablex, variabley) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  variabley <- evalParse(paste0('currentLattitude$`', variabley, '`'))
  chisq.test(variablex, variabley) %>% jsonTidy()
}

#' Fisher's exact test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param alternative alternative
#' @param confidencelevel confidencelevel
#' @post /fishertest
function(slides, names, range, firstrow, variablex, variabley, alternative='t', confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  variabley <- evalParse(paste0('currentLattitude$`', variabley, '`'))
  fisher.test(
    variablex,
    variabley,
    alternative=alternative,
    conf.level=evalParse(confidencelevel),
  ) %>% jsonTidy()
}

#' Cochran-Mantel-Haenszel Chi-Squared Test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param variablez variablez
#' @param alternative alternative
#' @param confidencelevel confidencelevel
#' @post /mantelhaentest
function(slides, names, range, firstrow, variablex, variabley, variablez, alternative='t', confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  variabley <- evalParse(paste0('currentLattitude$`', variabley, '`'))
  variablez <- evalParse(paste0('currentLattitude$`', variablez, '`'))
  mantelhaen.test(
    variablex,
    variabley,
    variablez,
    alternative=alternative,
    conf.level=evalParse(confidencelevel),
  ) %>% jsonTidy()
}

#' Correlation
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablesx variablesx to use
#' @param method method
#' @post /correlation
function(slides, names, range, firstrow, variablesx, method='p') {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  cor(
    currentLattitude[,fromJSON(variablesx)],
    # currentLattitude[,fromJSON(variablesy)],
    method=method,
  ) %>% jsonRownamesDf()
}

#' Covariance
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablesx variablesx to use
#' @param variablesy variablesy to use
#' @param method method
#' @post /covariance
function(slides, names, range, firstrow, variablesx, variablesy, method='p') {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  cov(
    currentLattitude[,fromJSON(variablesx)],
    currentLattitude[,fromJSON(variablesy)],
    method=method
  ) %>% jsonRownamesDf()
}

#' Testing correlation for significance
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param alternative alternative
#' @param method method
#' @param confidencelevel confidencelevel
#' @post /cortest
function(slides, names, range, firstrow, variablex, variabley, alternative='t', method='p', confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  variabley <- evalParse(paste0('currentLattitude$`', variabley, '`'))
  cor.test(
    variablex,
    variabley,
    alternative=alternative,
    method=method,
    conf.level=confidencelevel
  ) %>% jsonTidy()
}

#' One sample t-test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param alternative alternative
#' @param mean mean
#' @param confidencelevel confidencelevel
#' @post /ttest
function(slides, names, range, firstrow, variablex, alternative='t', mean=0, confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  t.test(
    variablex,
    alternative=alternative,
    mu=mean,
    conf.level=evalParse(confidencelevel)
  ) %>% jsonTidy()
}

#' Dependent t-test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param alternative alternative
#' @param mean mean
#' @param confidencelevel confidencelevel
#' @post /pairedttest
function(slides, names, range, firstrow, variablex, variabley, alternative='t', mean=0, confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  variabley <- evalParse(paste0('currentLattitude$`', variabley, '`'))
  t.test(
    variablex,
    variabley,
    alternative=alternative,
    mu=mean,
    conf.level=evalParse(confidencelevel),
    paired=T,
  ) %>% jsonTidy()
}

#' Independent Two-group t-test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param alternative alternative
#' @param mean mean
#' @param confidencelevel confidencelevel
#' @post /independentttest
function(slides, names, range, firstrow, variablex, variabley, alternative='t', mean=0, confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  variabley <- evalParse(paste0('currentLattitude$`', variabley, '`'))
  t.test(
    variablex,
    variabley,
    alternative=alternative,
    mu=mean,
    conf.level=evalParse(confidencelevel)
  ) %>% jsonTidy()
}

#' Independent Two-group Mann-Whitney U Test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param alternative alternative
#' @param mean mean
#' @param confidencelevel confidencelevel
#' @post /wilcoxtest
function(slides, names, range, firstrow, variablex, variabley, alternative='t', mean=0, confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  variabley <- evalParse(paste0('currentLattitude$`', variabley, '`'))
  wilcox.test(
    variablex,
    variabley,
    alternative=alternative,
    mu=mean,
    conf.level=evalParse(confidencelevel)
  ) %>% jsonTidy()
}

#' Paired Two-group Wilcoxon Signed Rank Test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param alternative alternative
#' @param mean mean
#' @param confidencelevel confidencelevel
#' @post /pairedwilcoxtest
function(slides, names, range, firstrow, variablex, variabley, alternative='t', mean=0, confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  variabley <- evalParse(paste0('currentLattitude$`', variabley, '`'))
  wilcox.test(
    variablex,
    variabley,
    alternative=alternative,
    mu=mean,
    conf.level=evalParse(confidencelevel),
    paired=T
  ) %>% jsonTidy()
}


#' Kruskal-Wallis rank sum test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param groups groups
#' @post /kruskaltest
function(slides, names, range, firstrow, variablex, groups) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  groups <- evalParse(paste0('currentLattitude$`', groups, '`'))
  kruskal.test(
    variablex,
    groups,
  ) %>% jsonTidy()
}

#' Friedman rank sum test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param groups groups
#' @param blocks blocks
#' @post /friedmantest
function(slides, names, range, firstrow, variablex, groups, blocks) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  groups <- evalParse(paste0('currentLattitude$`', groups, '`'))
  blocks <- evalParse(paste0('currentLattitude$`', blocks, '`'))
  friedman.test(
    variablex,
    groups,
    blocks,
  ) %>% jsonTidy()
}

#' Coefficients
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /coef
function(slides, names, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude) %>%
    coef() %>%
    jsonTidy()
}

#' Confidence Intervals
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @param confidencelevel confidence level
#' @post /confint
function(slides, names, range, firstrow, formula, confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude) %>%
    confint(level=evalParse(confidencelevel)) %>%
    jsonRownamesDf()
}

#' Fitted Values
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /fitted
function(slides, names, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude) %>%
    fitted() %>%
    jsonTidy()
}

#' Residuals
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /residuals
function(slides, names, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude) %>%
    residuals() %>%
    jsonTidy()
}

#' Covariance matrix
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /vcov
function(slides, names, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude)
    vcov() %>%
    jsonRownamesDf()
}

#' Akaike’s Information Criterion
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @param penalty penalty
#' @post /aic
function(slides, names, range, firstrow, formula, penalty=2) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude) %>%
    AIC(k=evalParse(penalty)) %>%
    toJSON(digits=NA)
}

#' Predict Response Values
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /predict
function(slides, names, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude) %>%
    predict() %>%
    jsonTidy()
}

#' Simple Linear Regression
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @post /simplelinreg
function(slides, names, range, firstrow, variablex, variabley) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(paste0('`', variabley, '`~`', variablex, '`')), currentLattitude) %>%
    jsonTidy()
}

#' Multiple Linear Regression
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /linreg
function(slides, names, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude) %>% jsonTidy()
}

#' Durbin–Watson test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @param lag maximum lag
#' @param method bootstrap method
#' @param alternative alternative
#' @post /durbinwatson
function(slides, names, range, firstrow, formula, lag=1, method='r', alternative='t') {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude) %>%
    dwt(
      max.lag=evalParse(lag),
      method=method,
      alternative=alternative
    ) %>%
    jsonTidy()
}

#' Non-Constant Error Variance score test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /ncvtest
function(slides, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lattitude <- lm(evalParse(formula), currentLattitude) %>%
    ncvTest()[-1] %>%
    unlist()
  matrix(c(names(lattitude), lattitude),nrow=2,byrow=T)
}

#' Bonferroni Outlier Test
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @param pvalue cutoff
#' @param observations maximum observations
#' @post /outliertest
function(slides, names, range, firstrow, formula, pvalue=0.05, observations=10) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  regress <- lm(evalParse(formula), currentLattitude) %>%
    outlierTest(
      cutoff=pvalue,
      n.max=observations
    ) %>%
    unlist()
  matrix(c(names(regress), regress),nrow=2,byrow=T)
}

#' Variance inflation factors
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /varianceinflation
function(slides, names, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  lm(evalParse(formula), currentLattitude) %>%
    vif() %>%
    jsonTidy()
}

#' ANOVA
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /anova
function(slides, names, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  jsonTidyAov(formula)
}

#' One-Way ANOVA
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @post /onewayaov
function(slides, names, range, firstrow, variablex, variabley) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  paste0('`', variabley, '`~`', variablex, '`') %>%
    jsonTidyAov()
}

#' Tukey multiple comparisons of means
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @param confidencelevel confidence level
#' @post /tukeyhsd
function(slides, names, range, firstrow, formula, confidencelevel=0.95) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  aov(evalParse(formula), currentLattitude) %>%
    TukeyHSD(conf.level=evalParse(confidencelevel)) %>%
    jsonTidy()
}

#' One-way ANCOVA with 1 covariate
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param covariate1 covariate1
#' @post /ancovawith1cov
function(slides, names, range, firstrow, variablex, variabley, covariate1) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  paste0('`', variabley, '`~`', covariate1, '`+`', variablex, '`') %>%
    jsonTidyAov()
}

#' Two-way factorial ANOVA
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variabley variabley
#' @param variablex1 variablex1
#' @param variablex2 variablex2
#' @post /twowayaov
function(slides, names, range, firstrow, variabley, variablex1, variablex2) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  paste0('`', variabley, '`~`', variablex1, '`*`', variablex2, '`') %>%
    jsonTidyAov()
}

#' Two-way factorial ANCOVA with 2 covariates
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variabley variabley
#' @param variablex1 variablex1
#' @param variablex2 variablex2
#' @param covariate1 covariate1
#' @param covariate2 covariate2
#' @post /twowayancovawith2cov
function(slides, names, range, firstrow, variabley, variablex1, variablex2, covariate1, covariate2) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  paste0('`', variabley, '`~`', covariate1, '`+`', covariate2, '`+`', variablex1, '`*`', variablex2, '`') %>%
    jsonTidyAov()
}

#' Randomized block design ANOVA
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param blocks blocking factor
#' @post /randomaov
function(slides, names, range, firstrow, variablex, variabley, blocks) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  paste0('`', variabley, '`~`', blocks, '`+`', variablex, '`') %>%
    jsonTidyAov()
}

#' One-way within-groups ANOVA
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @param subjects subjects
#' @post /onewaywithinaov
function(slides, names, range, firstrow, variablex, variabley, subjects) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  paste0('`', variabley, '`~`', variablex, '`+Error(`', subjects, '`/`', variablex, '`)') %>%
    jsonTidyAov()
}

#' Repeated measures ANOVA with 1 between- & within-groups factor
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex1 within-groups factpr
#' @param variablex2 between-groups factor
#' @param variabley variabley
#' @param subjects subjects
#' @post /repeatedmeasuresaov
function(slides, names, range, firstrow, variablex1, variablex2, variabley, subjects) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  paste0('`', variabley, '`~`', variablex2, '`*`', variablex1, '`+Error(`', subjects, '`/`', variablex1, '`)') %>%
    jsonTidyAov()
}

#' MANOVA
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param formula formula
#' @post /manova
function(slides, names, range, firstrow, formula) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  jsonTidyManova(formula)
}

#' One-way MANOVA
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variablesy variablesy to use
#' @post /onewayman
function(slides, names, range, firstrow, variablex, variablesy) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  paste0(
    'cbind(',
    paste(paste0('`', fromJSON(variablesy), '`'), collapse=','),
    ')~`', variablex, '`'
  ) %>% jsonTidyManova()
}

#' Robust One-way MANOVA
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variablesy variablesy to use
#' @param method method
#' @param approximation approximation
#' @post /robustonewayman
function(slides, names, range, firstrow, variablex, variablesy, method='c', approximation='B') {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  variablex <- evalParse(paste0('currentLattitude$`', variablex, '`'))
  variablesy <- evalParse(paste0(
    'cbind(',
    paste(paste0('currentLattitude$`', fromJSON(variablesy), '`'), collapse=','),
    ')'
  ))
  Wilks.test(
    variablesy,
    variablex,
    method=method,
    approximation=approximation
  ) %>% jsonTidy()
}

#' Bartlett Test Of Homogeneity Of Variances
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @post /bartletttest
function(slides, names, range, firstrow, variablex, variabley) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  bartlett.test(
    evalParse(paste0('`', variabley, '`~`', variablex, '`')),
    currentLattitude,
  ) %>% jsonTidy()
}

#' Fligner-Killeen Test Of Homogeneity Of Variances
#' @param slides worksheet data
#' @param names array of sheet names
#' @param range current sheet range
#' @param firstrow Should set first row as headers?
#' @param variablex variablex
#' @param variabley variabley
#' @post /flignertest
function(slides, names, range, firstrow, variablex, variabley) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
  dataNamesNoRewrite <<- fromJSON(names)
  currentLattitude <<- createLattitudeFromRange(range)
  currentLattitude <<- lattitudeAsDataframe(firstrow, currentLattitude, range)
  fligner.test(
    evalParse(paste0('`', variabley, '`~`', variablex, '`')),
    currentLattitude,
  ) %>% jsonTidy()
}

createDecision <- function(matchSlide, matchNumbers) {
  rowLength <- matchNumbers[2]-matchNumbers[1]+1
  colLength <- matchNumbers[4]-matchNumbers[3]+1
  if (colLength > 1) {
    resultList <- matrix(, nrow=rowLength, ncol=colLength)
    for (row in 1:rowLength) {
      for (col in 1:colLength) {
        resultList[row, col] <- paste0(matchSlide, '[', matchNumbers[1]+row-1, ',', matchNumbers[3]+col-1, ']')
      }
    }
    return(as.vector(resultList))
  } else {
    resultList <- vector(mode='character', length=rowLength)
    for (row in 1:rowLength) {
      resultList[row] <- paste0(matchSlide, '[', matchNumbers[1]+row-1, ',', matchNumbers[3], ']')
    }
    return(resultList)
  }
}

checkContainsMatrix <- function(customfunction, ...) {
  result <- customfunction(...)
  if (length(result) > 1) {
    decisionContainsMatrix <<- T
  }
  result[1]
}

createLattitudeFromDecision <- function(currentSlide) {
  # Only vectors allowed, no matrix
  currentLattitude <- sapply(currentSlide, function(x) checkContainsMatrix(configureAndCompute, x))
  # Check which sheet is used
  checklist <- dataNamesNoRewrite
  for (n in dataNamesNoRewrite) {
    if (any(grepl(n, currentLattitude))) {
      checklist <- checklist[checklist != n]
    }
  }
  checklist <- match(dataNamesNoRewrite, checklist)
  for (n in 1:length(checklist)) {
    if (is.na(checklist[n])) {
      # Previous sheets might be NA from configureAndCompute
      configureSheetsToMatrix()
      currentLattitude <- evalParse(paste0('`', dataNamesNoRewrite[n], '`'))
      currentLattitude <- apply(currentLattitude, 1:2, function(x) checkContainsMatrix(configureAndCompute, x))
      eval(call('<<-', as.name(dataNamesNoRewrite[n]), currentLattitude))
    }
  }
  # Must splitAndReplace cells to level 1 cell reference. eval(call()) will replace sheets to numeric causing cell reference to return NA
  currentLattitude <- sapply(currentSlide, function(x) checkContainsMatrix(splitAndReplace, x, NULL))
  # decision results must be a vector, not matrix.
  sapply(currentLattitude, function(x) checkContainsMatrix(numerizeAndSplit, x, x))
}

evaluateCellExceptDecision <- function(cell) {
  if (notApplicable(cell) || cell %in% parametersListWithAndWithoutQuotes) return(cell)
  result <- cell
  try(
    # cell != NA returns NA instead of TRUE/FALSE
    while(!identical(result, evalParse(result))) {
      # mean(test.csv[,1:1]) not mean(sheet1[,1:1]) (cell referenced: 'sheet1[2:2]')
      if (grepl(FULL_COLUMN_ROW, result)
        && gregexpr('(', result, fixed=T)[[1]] != -1
        && length(gregexpr('(', result, fixed=T)[[1]]) == 1
        && gregexpr(')', result, fixed=T)[[1]] != -1
        && length(gregexpr(')', result, fixed=T)[[1]])) {
        nextResult <- tryCatch({
          resultNaRm <- substr(result, 1, nchar(result)-1)
          resultNaRm <- paste0(resultNaRm, ',na.rm=T)')
          if (!(is.numeric(resultNaRm) && length(resultNaRm) == 1)) {
            # cbind(Intercept=1,fibrinogen=test.csv[,1:1]
            # If other columns are longer, will take in the longer length. Leads to trailing Intercept 1's.
            resultNaRm <- evalParse(result)
          }
          resultNaRm
        }, error = function(cond) NA)
      } else {
        nextResult <- evalParse(result)
      }
      # simple_triplet_matrix(1,1,1)
      if (is.simple_triplet_matrix(nextResult)) {
        result <- as.matrix(nextResult)
        break
      }
      if (is.na(nextResult) || result %in% parametersListWithAndWithoutQuotes) break
      result <- nextResult
      if (length(result) > 1) break
    },
  silent=T)
  # length(result) > 1 prevents evaluateCellExceptDecision(c(1:9)) from returning c(1:9)
  if(is.null(result) || is.function(result) || length(result) > 1) return(cell)
  return(result)
}

splitAndReplaceExceptDecision <- function(cell, dataname) {
  # matrix(c(1:9), 3)
  # evaluateCellExceptDecision would return numeric when we don't want it to.
  # cell <- evaluateCellExceptDecision(cell)
  if (notApplicable(cell) || is.numeric(cell) || length(cell) > 1) return(cell)
  if (is.factor(cell)) {
    return(as.numeric(cell))
  }
  stringParts <- strsplit(cell, EQUATION_FORM_SPLIT, perl=T)[[1]]
  # sapply used to distinguish with and without quotes simultaneously 'sum(mean(test.csv[,2:2], na.rm=T),`test.csv`[3,3])'
  stringParts <- unlist(sapply(stringParts, function(x) {
    if (!is.null(dataname)) {
      datanameWithQuote <- paste0('`', dataname, '`')
      if (grepl(datanameWithQuote, x)) {
        dataNameRegex <- paste0('\\({1}(?!', datanameWithQuote, ')')
      } else {
        dataNameRegex <- paste0('\\({1}(?!', dataname, ')')
      }
    }
    if (!grepl(FULL_COLUMN_ROW, x) && grepl('\\(|\\)', x)) {
      if (!is.null(dataname)) {
        dataNameRegex <- paste0(dataNameRegex, '|\\(|\\)')
      } else {
        dataNameRegex <- '\\(|\\)'
      }
      return(strsplit(x, dataNameRegex, perl=T)[[1]])
    } else {
      return(x)
    }
  }, USE.NAMES=F))
  evaluateParts <- sapply(stringParts, evaluateCellExceptDecision, USE.NAMES=F)
  # Matrix should not produce a list for splitAndReplaceExceptDecision
  if (identical(typeof(evaluateParts), 'list')) {
    evaluateParts <- stringParts
  }
  newCell <- cell
  # Placing brackets to account for order of operations when accounting for minus
  for (part in 1:length(stringParts)) {
    if(!identical(stringParts[part],'')) {
      negativeString <- paste0('-', stringParts[part])
      matchNegative <- grep(negativeString, newCell, fixed=T)
      if (length(matchNegative) > 0) {
        negativeEvaluate <- paste0('(', evaluateParts[part], ')')
        newCell <- sub(stringParts[part], negativeEvaluate, newCell, fixed=T)
      } else {
        newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed=T)
      }
    }
  }
  if (decisionContainsMatrix) {
    # replace parameters with x
    # for (par in 1:length(parametersList)) {
    #   newCell <- gsub(parametersList[par], 'x', newCell, fixed=T)
    #   newCell <- gsub(parametersListOnlyQuotes[par], 'x', newCell, fixed=T)
    # }
    # To check: may produce unwanted results by replacing single cell sheet1[1,1] as well as sheet1[1:2,1] with x
    for (par in parametersListWithAndWithoutQuotesWithMatrix) {
      newCell <- gsub(par, 'x', newCell, fixed=T)
    }
    # for (par in parametersAsMatrix) {
    #   newCell <- gsub(par, 'x', newCell, fixed=T)
    # }
  } else {
    # replace parameters with x[1], x[2]
    # for (par in 1:length(parametersList)) {
    #   newPar <- paste0('x[',par,']')
    #   newCell <- gsub(parametersList[par], newPar, newCell, fixed=T)
    #   newCell <- gsub(parametersListOnlyQuotes[par], newPar, newCell, fixed=T)
    # }
    for (par in parametersListWithAndWithoutQuotes) {
      newCell <- gsub(par, paste0('x[',par,']'), newCell, fixed=T)
    }
  }
  newCell
}

numerizeAndSplitExceptDecision <- function(cell, original) {
  # 'Sheet1!C3 -- Sheet1!C4 -- Dec-31 or 0.42 or 1+3'
  # 'Sheet1!C3 -- Sheet1!C4 + Sheet1!C5 -- 0.43'
  if (notApplicable(cell) || is.numeric(cell) || !hasCellReference(cell)) {
    return(cell)
  }
  # 'Sheet1!C3 + Sheet1!C6 -- 42 or Dec-31+0.1919'
  # 'Sheet1!C3 -- Dec-31'
  for (slide in 1:length(dataNamesNoRewrite)) {
    name <- as.name(dataNamesNoRewrite[slide])
    longitude <- applyNumeric(dataNamesNoRewrite[slide])
    # 'Sheet1!C3 -- mean(Sheet1$Rating_X) + 0.43'
    if (grepl('$', cell, fixed=T)) {
      eval(call('<<-', name, as.data.frame(longitude, stringsAsFactors=F)))
    # 'Sheet1!C3 -- mean(Sheet1[,2:3]) + 0.43'
    } else {
      eval(call('<<-', name, longitude))
    }
    # Without cell reference: 'mean(Sheet1[2:3,2:2]) + mean(Sheet1[3:4,2:2])'
    # Without cell reference: 'Sheet4[1:3,1:3] %*% Sheet4[1:3,2:4]'
    # 'mean(Sheet4$Rating_X)' will not work
    newStoredString <- splitAndReplaceExceptDecision(cell, dataNamesNoRewrite[slide])
    # Repeats when sheetNames ['test.csv', 'sheet1'] instead of ['sheet1', 'test.csv']
    if (!identical(newStoredString, cell)) return(newStoredString)
  }
  # 'sheet1[1,1:9]' -- c(1:9) numeric matrix should remain
  cell
}

createGeneralFormFunction <- function(cell) {
  if (notApplicable(cell)) return(cell)
  configureSheetsToMatrix()
  # Matrix multiplication expected to be numeric so cell should not be evaluated prior to applyNumeric
  # Should not evaluateCellExceptDecision prior to splitAndReplaceExceptDecision, only first element will be used
  storedString <- cell
  if (!grepl('%*%', cell)) {
    nextStoredString <- splitAndReplaceExceptDecision(storedString, NULL)
    while(storedString != nextStoredString) {
      storedString <- nextStoredString
      if (grepl('%*%', storedString) || length(storedString) > 1) break
      nextStoredString <- splitAndReplaceExceptDecision(storedString, NULL)
    }
  }
  numerizeAndSplitExceptDecision(storedString, cell)
}

isValidFormula <- function(formula) {
  for (par in 1:length(parametersList)) {
    if (grepl(paste0('x[', par, ']'), formula, fixed=T) || grepl(paste0('x'), formula, fixed=T)) {
      return(T)
    }
  }
  return(F)
}

createLattitudeFromSlide <- function(name) {
  configureSheetsToMatrix()
  currentSlide <- evaluateCell(name)
  currentLattitude <- apply(currentSlide, 1:2, function(x) configureAndCompute(x)[1])
  # Check which sheet is used
  checklist <- dataNamesNoRewrite
  for (n in dataNamesNoRewrite) {
    if (any(grepl(n, currentLattitude))) {
      checklist <- checklist[checklist != n]
    }
  }
  checklist <- match(dataNamesNoRewrite, checklist)
  for (n in 1:length(checklist)) {
    # currentSlide is the entire sheet instead of range, so no need to recalculate unlike createLattitudeFromRange.
    if (is.na(checklist[n]) && dataNamesNoRewrite[n] != name) {
      # Previous sheets might be NA from configureAndCompute
      configureSheetsToMatrix()
      currentLattitude <- evalParse(paste0('`', dataNamesNoRewrite[n], '`'))
      currentLattitude <- apply(currentLattitude, 1:2, function(x) configureAndCompute(x)[1])
      eval(call('<<-', as.name(dataNamesNoRewrite[n]), currentLattitude))
    }
  }
  # Must splitAndReplace cells to level 1 cell reference. eval(call()) will replace sheets to numeric causing cell reference to return NA
  currentLattitude <- apply(currentSlide, 1:2, function(x) splitAndReplace(x, NULL)[1])
  apply(currentLattitude, 1:2, function(x) numerizeAndSplit(x, x)[1])
}

columnToLetter <- function(column) {
  letter <- ''
  while (column > 0) {
    temp <- (column - 1) %% 26
    letter <- paste0(letters[column], letter)
    column <- (column - temp - 1) / 26
  }
  return(toupper(letter))
}

translateDirection <- function(fdir) {
  sapply(createLattitudeFromRange(fdir), function(x) {
    if (!(x == '==' || x == '=' || x == '<=' || x == '>=')) {
      stop('Direction of constraint must be ==, =, <= or >=.')
    }
    if (x == '=') {
      return('==')
    } else {
      return(x)
    }
  })
}

createRhsConstraint <- function(range) {
  result <- sapply(createLattitudeFromRange(range), as.numeric, USE.NAMES=F)
  if (any(is.na(result))) {
    stop('Right hand side of constraint must be numeric.')
  }
  return(result)
}

addConstraintsAsVector <- function(const) {
  if (exists('constraints_vector')) {
    return(c(constraints_vector, const))
  } else {
    return(const)
  }
}

addBoundsAsVector <- function(const) {
  if (exists('bounds_vector')) {
    return(c(bounds_vector, const))
  } else {
    return(const)
  }
}

createConeConstraint <- function(lhs, cone, rhs, K) {
  lhs <- createLattitudeFromRange(lhs)
  if (is.vector(lhs)) {
    lhs <- sapply(lhs, as.numeric, USE.NAMES=F)
  } else {
    lhs <- apply(lhs, 1:2, as.numeric)
  }
  if (any(is.na(lhs))) {
    stop('Left hand side of conic constraint must be numeric.')
  }
  rhs <- sapply(createLattitudeFromRange(rhs), as.numeric, USE.NAMES=F)
  if (any(is.na(rhs))) {
    stop('Right hand side of conic constraint must be numeric.')
  }
  cone <- as.numeric(createLattitudeFromRange(cone))
  if (length(cone) > 1) {
    stop('Cone must be numeric (e.g., 1 for K_expp(1), 1/4 for K_powp(1/4)).')
  }
  C_constraint(L=lhs, cone=evalParse(paste0(K, '(', cone, ')')), rhs=rhs)
}

nested_unclass <- function(x) {
  x <- unclass(x)
  if (is.list(x)) {
    x <- lapply(x, nested_unclass)
  }
  return(x)
}

#' Optimization
#' @param slides worksheet data
#' @param names array of sheet names
#' @param objective cell we want to optimize
#' @param quadratic quadratic portion of the objective
#' @param linear linear portion of the objective
#' @param gradient gradient of the objective
#' @param hessian hessian of the objective
#' @param minmax whether to maximize or minimize objective
#' @param decision decision variables to Manipulate
#' @param flhs left hand side of general constraints
#' @param fdir direction of general constraints
#' @param frhs right hand side of general constraints
#' @param blhs left hand side of bounds
#' @param bdir direction of bounds
#' @param brhs right hand side of bounds
#' @param lowerindex lower index of bounds
#' @param lowerbound lower bounds of bounds
#' @param upperindex upper index of bounds
#' @param upperbound upper bounds of bounds
#' @param lowerlimit lower limit of bounds
#' @param upperlimit upper limit of bounds
#' @param jacobian jacobian of general constraints
#' @param qquad quadratic portion of qaudratic constraints
#' @param qlin linear portion of qaudratic constraints
#' @param qdir direction of qaudratic constraints
#' @param qrhs right hand side of qaudratic constraints
#' @param llin linear portion of linear constraints
#' @param ldir direction of linear constraints
#' @param lrhs right hand side of linear constraints
#' @param c0lhs left hand side of zero cone
#' @param c0cone cone value in K_zero of zero cone
#' @param c0rhs right hand side of zero cone
#' @param cllhs left hand side of linear cone
#' @param lcone cone value in K_lin of zero cone
#' @param clrhs right hand side of linear cone
#' @param csolhs left hand side of second-order cone
#' @param socone cone value in K_soc of zero cone
#' @param csorhs right hand side of second-order cone
#' @param cexlhs left hand side of exponnetial cone
#' @param excone cone value in K_expp of zero cone
#' @param cexrhs right hand side of exponnetial cone
#' @param cpplhs left hand side of power 3d cone
#' @param ppcone cone value in K_powp of zero cone
#' @param cpprhs right hand side of power 3d cone
#' @param cpdlhs left hand side of power 2d cone
#' @param pdcone cone value in K_powd of zero cone
#' @param cpdrhs right hand side of power 2d cone
#' @param cpsdlhs left hand side of positive semidefinite cone
#' @param psdcone cone value in K_psd of zero cone
#' @param cpsdrhs right hand side of positive semidefinite cone
#' @param solver solver
#' @post /optimization
function(slides, names, objective, quadratic, linear, gradient, hessian, minmax, decision, flhs, fdir, frhs, blhs, bdir, brhs,
  lowerindex, lowerbound, upperindex, upperbound, lowerlimit, upperlimit, jacobian, qquad, qlin, qdir, qrhs, llin, ldir, lrhs,
  c0lhs, c0cone, c0rhs, cllhs, lcone, clrhs, csolhs, socone, csorhs, cexlhs, excone, cexrhs, cpplhs, ppcone, cpprhs, cpdlhs,
  pdcone, cpdrhs, cpsdlhs, psdcone, cpsdrhs, solver) {
  slidesNoRewrite <<- fromJSON(slides, simplifyMatrix=F)
	dataNamesNoRewrite <<- fromJSON(names)
  configureSheetsToMatrix()
  optimization <- OP()
  # General Objective
  if (objective != 'na') {
    decisionParsed <- evaluateCell(decision)
    # Decision must be limited to a single row or column. Safeguards in client side
    # if (is.matrix(decisionParsed)) {
    #   stop('Decision variables must be limited to a single row or column.')
    # }
    matchNumbers <- appendMatchNumbers(decision)
    if (length(matchNumbers) != 4) {
      stop('Invalid decision variables.')
    }
    # Decision variables should be numeric unless it represents a matrix
    # We don't want to evalute. Just need the cell reference.
    parametersList <<- createDecision(sub(RANGE_REGEX, '', decision), matchNumbers)
    parametersListOnlyQuotes <<- createDecision(rangeToMatchSlideQuotes(decision), matchNumbers)
    parametersListWithAndWithoutQuotes <<- c(parametersList, parametersListOnlyQuotes)
    parametersListWithAndWithoutQuotesWithMatrix <<- c(
      parametersListWithAndWithoutQuotes,
      decision,
      paste0(
        rangeToMatchSlideQuotes(decision),
        regmatches(decision, regexpr(RANGE_REGEX, decision))
      )
    )
    parametersAsMatrix <<- c(decision, paste0(
      rangeToMatchSlideQuotes(decision),
      regmatches(decision, regexpr(RANGE_REGEX, decision))
    ))
    ndecision <- length(parametersList)
    # decides whether to replace decision with x or x[1] in splitAndReplaceExceptDecision
    decisionContainsMatrix <<- F
    startDecision <- createLattitudeFromDecision(decisionParsed)
    objectiveFormula <- createGeneralFormFunction(objective)
    if (!isValidFormula(objectiveFormula)) {
      stop('Decision variable(s) missing from the objective.')
    }
    # F_objective requires general function variables to be numeric
    for (name in dataNamesNoRewrite) {
      if (grepl(name, objectiveFormula)) {
        currentLattitude <- createLattitudeFromSlide(name)
        currentLattitude <- apply(currentLattitude, 1:2, as.numeric)
        eval(call('<-', as.name(name), currentLattitude))
      }
    }
    # Can't do sum(Sheet4[,2:2]*3) -- sum(Sheet4[1,2]*3, Sheet4[2,2]*3...)
    objectiveFormula <- evalParse(paste0('function(x){', objectiveFormula, '}'))
    # Gradient
    if (gradient != 'na') {
      # Gradient could be constant
      gradientFormula <- createGeneralFormFunction(gradient)
      gradientFormula <- evalParse(paste0('function(x){', gradientFormula, '}'))
    }
    # Hessian
    if (hessian != 'na') {
      hessianFormula <- createGeneralFormFunction(hessian)
      hessianFormula <- evalParse(paste0('function(x){', hessianFormula, '}'))
    }
    if (gradient != 'na' && hessian != 'na') {
      objective(optimization) <- F_objective(objectiveFormula, ndecision, G=gradientFormula, H=hessianFormula, names=parametersList)
    } else if (gradient != 'na' && hessian == 'na') {
      objective(optimization) <- F_objective(objectiveFormula, ndecision, G=gradientFormula, names=parametersList)
    } else if (gradient == 'na' && hessian != 'na') {
      objective(optimization) <- F_objective(objectiveFormula, ndecision, H=hessianFormula, names=parametersList)
    } else {
      objective(optimization) <- F_objective(objectiveFormula, ndecision, names=parametersList)
    }
  # Quadratic objective
  } else if (quadratic != 'na') {
    # Quadratic must a matrix, not single row or column. Safeguards placed from client side.
    matchNumbers <- appendMatchNumbers(quadratic)
    if (length(matchNumbers) != 4) {
      stop('Invalid range for quadratic portion of the objective.')
    }
    # create names from quadratic columns
    colLength <- matchNumbers[4]-matchNumbers[3]+1
    quadraticColumns <- vector(mode='character', length=colLength)
    matchSlide <- sub(RANGE_REGEX, '', quadratic)
    for (col in 1:colLength) {
      quadraticColumns[col] <- paste0(matchSlide, '[', matchNumbers[1], ':', matchNumbers[2], ',', matchNumbers[3]+col-1, ']')
    }
    names <- sapply(quadraticColumns, function(cell) {
      cell <- regmatches(cell, regexpr(RANGE_REGEX, cell))
      cell <- regmatches(cell, gregexpr('\\d+', cell))[[1]]
      letter <- columnToLetter(as.numeric(cell[3]))
      return(paste0(gsub('`', "'", matchSlide), letter, cell[1], ':', letter, cell[2]))
    }, USE.NAMES=F)
    quadratic <- apply(createLattitudeFromRange(quadratic), 1:2, as.numeric)
    # No need to check for length(quadratic). Already checked in client
    if (any(is.na(quadratic))) {
      stop('Quadratic portion of objective must be numeric.')
    }
    if (linear != 'na') {
      # Linear must be limited to a single row or column. Safeguards in client side
      matchNumbers <- appendMatchNumbers(linear)
      if (length(matchNumbers) != 4) {
        stop('Invalid range for linear portion of the objective.')
      }
      linear <- sapply(createLattitudeFromRange(linear), as.numeric, USE.NAMES=F)
      if (any(is.na(linear))) {
        stop('Linear portion of objective must be numeric.')
      }
      objective(optimization) <- Q_objective(Q=quadratic, L=linear, names=names)
    } else {
      objective(optimization) <- Q_objective(Q=quadratic, names=names)
    }
    ndecision <- ncol(quadratic)
  # Linear Objective
  } else if (linear != 'na' && quadratic == 'na') {
    # Linear must be limited to a single row or column. Safeguards in client side
    matchNumbers <- appendMatchNumbers(linear)
    if (length(matchNumbers) != 4) {
      stop('Invalid range for linear objective.')
    }
    matchSlide <- sub(RANGE_REGEX, '', linear)
    names <- sapply(createDecision(matchSlide, matchNumbers), function(cell) {
      cell <- regmatches(cell, regexpr(RANGE_REGEX, cell))
      cell <- regmatches(cell, gregexpr('\\d+', cell))[[1]]
      return(paste0(gsub('`', "'", matchSlide), columnToLetter(as.numeric(cell[2])), cell[1]))
    }, USE.NAMES=F)
    linear <- sapply(createLattitudeFromRange(linear), as.numeric, USE.NAMES=F)
    if (any(is.na(linear))) {
      stop('Linear objective must be numeric.')
    }
    objective(optimization) <- L_objective(linear, names=names)
    ndecision <- length(linear)
  }

  if (flhs != 'na' && fdir != 'na' && frhs != 'na') {
    matchNumbers <- appendMatchNumbers(flhs)
    if (length(matchNumbers) != 4) {
      stop('Invalid range for left hand side of general constraint.')
    }
    flhs <- createDecision(sub(RANGE_REGEX, '', flhs), matchNumbers)
    flhs <- sapply(flhs, createGeneralFormFunction, USE.NAMES=F)
    if (F %in% sapply(flhs, isValidFormula, USE.NAMES=F)) {
      stop('Constraints must contain a decision variable')
    }
    flhs <- sapply(flhs, function(x) {
      evalParse(paste0('function(x){', x, '}'))
    })
    fdir <- translateDirection(fdir)
    frhs <- createRhsConstraint(frhs)
    if (jacobian != 'na') {
      # Constant Jacobian exists
      jacobianFormula <- createGeneralFormFunction(gradient)
      jacobianFormula <- evalParse(paste0('function(x){', jacobianFormula, '}'))
      constraints_vector <- F_constraint(F=flhs, dir=fdir, rhs=frhs, J=jacobianFormula, names=parametersList)
    } else {
      constraints_vector <- F_constraint(F=flhs, dir=fdir, rhs=frhs, names=parametersList)
    }
  }

  if (qquad != 'na' && qdir != 'na' && qrhs != 'na') {
    # list of length m where the entries are either of n × n matrices or NULL.
    # NOT WORKING YET
    qquad <- apply(createLattitudeFromRange(qquad), 1:2, as.numeric)
    # No need to check for length(qquad). Already checked in client
    if (any(is.na(qquad))) {
      stop('Quadratic portion of constraint must be numeric.')
    }
    qdir <- translateDirection(qdir)
    qrhs <- createRhsConstraint(qrhs)
    if (qlin != 'na') {
      qlin <- sapply(createLattitudeFromRange(qlin), as.numeric, USE.NAMES=F)
      if (any(is.na(qlin))) {
        stop('Linear objective must be numeric.')
      }
      constraints_vector <- addConstraintsAsVector(Q_constraint(Q=qquad, L=qlin, dir=qdir, rhs=qrhs))
    } else {
      constraints_vector <- addConstraintsAsVector(Q_constraint(Q=qquad, dir=qdir, rhs=qrhs))
    }
  }

  if (llin != 'na' && ldir != 'na' && lrhs != 'na') {
    llin <- sapply(createLattitudeFromRange(llin), as.numeric, USE.NAMES=F)
    if (any(is.na(llin))) {
      stop('Linear constraint must be numeric.')
    }
    constraints_vector <- addConstraintsAsVector(
      L_constraint(L=llin, dir=translateDirection(ldir), rhs=createRhsConstraint(lrhs)))
  }

  if (c0lhs != 'na' && c0cone != 'na' && c0rhs != 'na') {
    constraints_vector <- addConstraintsAsVector(createConeConstraint(c0lhs, c0cone, c0rhs, 'K_zero'))
  }
  if (cllhs != 'na' && lcone != 'na'  && clrhs != 'na') {
    constraints_vector <- addConstraintsAsVector(createConeConstraint(cllhs, lcone, clrhs, 'K_lin'))
  }
  if (csolhs != 'na' && socone != 'na' && csorhs != 'na') {
    constraints_vector <- addConstraintsAsVector(createConeConstraint(csolhs, socone, csorhs, 'K_soc'))
  }
  if (cexlhs != 'na' && excone != 'na' && cexrhs != 'na') {
    constraints_vector <- addConstraintsAsVector(createConeConstraint(cexlhs, excone, cexrhs, 'K_expp'))
  }
  if (cpplhs != 'na' && ppcone != 'na' && cpprhs != 'na') {
    constraints_vector <- addConstraintsAsVector(createConeConstraint(cpplhs, ppcone, cpprhs, 'K_powp'))
  }
  if (cpdlhs != 'na' && pdcone != 'na' && cpdrhs != 'na') {
    constraints_vector <- addConstraintsAsVector(createConeConstraint(cpdlhs, pdcone, cpdrhs, 'K_powd'))
  }
  if (cpsdlhs != 'na' && psdcone != 'na' && cpsdrhs != 'na') {
    constraints_vector <- addConstraintsAsVector(createConeConstraint(cpsdlhs, psdcone, cpsdrhs, 'K_psd'))
  }
  if (exists('constraints_vector')) {
    constraints(optimization) <- constraints_vector
  }

  # Bounds blhs, bdir, brhs only for general form function
  if (blhs != 'na' && bdir != 'na' && brhs != 'na' && objective != 'na') {
    # matchNumbers <- appendMatchNumbers(blhs)
    # if (length(matchNumbers) != 4) {
    #   stop('Invalid range for left hand side of bounds.')
    # }
    # blhs <- createDecision(sub(RANGE_REGEX, '', blhs), matchNumbers)
    bdir <- translateDirection(bdir)
    brhs <- createRhsConstraint(brhs)
    for (i in 1:length(blhs)) {
    	if (identical(bdir[i], '>=') && !identical(brhs[i],0)) {
    		index <- regmatches(blhs[i], regexpr('\\d+', blhs[i]))
    		if (identical(bdir[i], '=')) {
          bounds_vector <- addBoundsAsVector(
            V_bound(li=index, ui=index, lb=brhs[i], ub=brhs[i], nobj=length(parametersList), names=parametersList))
    		} else if (identical(bdir[i], '>=')) {
          bounds_vector <- addBoundsAsVector(
            V_bound(li=index, ui=index, lb=brhs[i], nobj=length(parametersList), names=parametersList))
    		} else if (identical(bdir[i], '<=')) {
          bounds_vector <- addBoundsAsVector(
            V_bound(li=index, ui=index, ub=brhs[i], nobj=length(parametersList), names=parametersList))
    		}
    	}
    }
    if (exists('bounds_vector')) {
      bounds(optimization) <- bounds_vector
    }
  }

  # Bounds lb, ub, ld, ud
  if (lowerbound != 'na' || upperbound != 'na' || lowerlimit != 'na' || upperlimit != 'na') {
    if (lowerindex == 'na') {
      lowerindex <- NULL
    } else {
      lowerindex <- sapply(createLattitudeFromRange(lowerindex), as.numeric, USE.NAMES=F)
      if (any(is.na(lowerindex))) {
        stop('Lower index must be numeric.')
      }
    }
    if (upperindex == 'na') {
      upperindex <- NULL
    } else {
      upperindex <- sapply(createLattitudeFromRange(upperindex), as.numeric, USE.NAMES=F)
      if (any(is.na(upperindex))) {
        stop('Upper index must be numeric.')
      }
    }
    if (lowerbound == 'na') {
      lowerbound <- NULL
    } else {
      lowerbound <- sapply(createLattitudeFromRange(lowerbound), as.numeric, USE.NAMES=F)
      if (any(is.na(lowerbound))) {
        stop('Lower bound must be numeric.')
      }
    }
    if (upperbound == 'na') {
      upperbound <- NULL
    } else {
      upperbound <- sapply(createLattitudeFromRange(upperbound), as.numeric, USE.NAMES=F)
      if (any(is.na(upperbound))) {
        stop('Upper bound must be numeric.')
      }
    }
    if (lowerlimit == 'na') {
      lowerlimit <- 0
    } else {
      lowerlimit <- sapply(createLattitudeFromRange(lowerlimit), as.numeric, USE.NAMES=F)
      if (any(is.na(lowerlimit))) {
        stop('Lower limit must be numeric.')
      }
    }
    if (upperlimit == 'na') {
      upperlimit <- Inf
    } else {
      upperlimit <- sapply(createLattitudeFromRange(upperlimit), as.numeric, USE.NAMES=F)
      if (any(is.na(upperlimit))) {
        stop('Upper limit must be numeric.')
      }
    }
    bounds(optimization) <- V_bound(
      li=lowerindex,
      ui=upperindex,
      lb=lowerbound,
      ub=upperbound,
      ld=lowerlimit,
      ud=upperlimit,
      nobj=ndecision,
    )
  }

	if (minmax == 1) {
		maximum(optimization) <- T
	}

  file <- tempfile()
  # only works for lp_solve
  # print(ROI_registered_writer())
  # ROI_write(optimization, file, solver=solver)
  # print(toJSON(readLines(file)))

  # Optimization
  if (objective != 'na') {
    solution <- ROI_solve(optimization, solver=solver, start=startDecision)
  } else {
    solution <- ROI_solve(optimization, solver=solver)
  }
	solution <- nested_unclass(solution)
	result <- matrix(c(solution$objval, solution$solution, solution$status$msg$solver), nrow=1, byrow=T)
  # Add gradient and hessian
	colnames(result) <- c('objective', names(solution$solution), 'method')
  print(as.data.frame(result))
	jsonString <- toJSON(as_tibble(result))
}

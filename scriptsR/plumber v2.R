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
library(ggplot2)
library(dplyr)
library(broom)
library(ROI)
library(ROI.plugin.optimx)
library(ROI.plugin.lpsolve)
library(ROI.plugin.quadprog)
library(ROI.plugin.nloptr)

#* @filter cors
cors <- function(res) {
  res$setHeader("Access-Control-Allow-Origin", "*")
  res$setHeader("Access-Control-Allow-Headers", "Content-Type")
  res$setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
  plumber::forward()
}

notApplicable <- function(cell) {
  is.null(cell) || is.na(cell) || cell==""
}

evaluateCell <- function(cell) {
  if (notApplicable(cell)) return(cell)
  result <- cell
  try(
    while(!identical(result, eval(parse(text=result)))) {
      if (is.na(eval(parse(text=result)))) break
      result <- eval(parse(text=result))
      if (length(result) > 1) break
    }, silent=T
  )
  if(is.null(result) || is.function(result)) return(cell)
  return(result)
}

EQUATION_SPLIT <- "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\(|\\)"

splitAndReplace <- function(cell) {
  if (notApplicable(cell) || is.numeric(cell)) return(cell)
  if (is.factor(cell)) {
    return(as.numeric(cell))
  }
  stringParts <- strsplit(cell, EQUATION_SPLIT)[[1]]
  evaluateParts <- sapply(
		stringParts,
		evaluateCell,
		USE.NAMES=F
	)
  # Some cases of matrix multiplication %*% produces list resulting in evaluateCell working when we don't want it to
  if (identical(typeof(evaluateParts), "list")) {
    evaluateParts <- stringParts
  }
  newCell <- cell
  for (part in 1:length(stringParts)) {
    if (!identical(stringParts[part],"")) {
      newCell <- sub(
        stringParts[part],
        evaluateParts[part],
        newCell,
        fixed=T
      )
    }
  }
  return(evaluateCell(newCell))
}

evaluateCellNoRewrite <- function(cell, actualData, sheetname) {
  if (notApplicable(cell)) return(cell)
  eval(call("<-", as.name(sheetname), actualData))
  result <- cell
  try(while(!identical(result,eval(parse(text=result)))) {
    if (is.na(eval(parse(text=result)))) break
    result <- eval(parse(text=result))
    if (length(result) > 1) break
  }, silent=T)
  if (is.null(result) || is.function(result)) return(cell)
  return(result)
}

splitAndReplaceNoRewrite <- function(cell, actualData, sheetname) {
  if (notApplicable(cell) || is.numeric(cell)) return(cell)
  if (is.factor(cell)) {
    return(as.numeric(cell))
  }
  stringParts <- strsplit(
    cell,
    EQUATION_SPLIT
  )[[1]]
  evaluateParts <- simplify2array(lapply(
    stringParts,
    evaluateCellNoRewrite,
    actualData=actualData,
    sheetname=sheetname
  ))
  newCell <- cell
  for (part in 1:length(stringParts)) {
    if(!identical(stringParts[part],"")) {
      newCell <- sub(
        stringParts[part],
        evaluateParts[part],
        newCell,
        fixed=T
      )
    }
  }
  return(evaluateCellNoRewrite(
    newCell,
    actualData=actualData,
    sheetname=sheetname
  ))
}

computeStandardR <- function(cell) {
  # find out which Sheet is used in the cell
  if (notApplicable(cell)) return(cell)
  storedString <- splitAndReplace(evaluateCell(cell))
  if (is.numeric(storedString)) return(storedString)
  if (is.factor(storedString)) {
    return(as.numeric(storedString))
  }
  for (slide in 1:length(sheetNamesNoRewrite)) {
    # "Sheet1!C3 -- mean(Sheet1$Rating_X) + 0.43"
    if (grepl("$", storedString, fixed=T)) {
      eval(call("<-",
        as.name(sheetNamesNoRewrite[slide]),
        as.data.frame(applyNumeric(sheetNamesNoRewrite[slide]), stringsAsFactors=F)
      ))
    # "Sheet1!C3 -- mean(Sheet1[,2:3]) + 0.43"
    } else {
      eval(call("<-",
        as.name(sheetNamesNoRewrite[slide]),
        applyNumeric(sheetNamesNoRewrite[slide])
      ))
    }
    # Don't return cell, "mean(Sheet4$Rating_X)" will not work
    sheetnameAsString <- paste0("`", sheetNamesNoRewrite[slide], "`")
    newStoredString <- splitAndReplaceNoRewrite(
      storedString,
      eval(parse(text=sheetnameAsString)),
      sheetNamesNoRewrite[slide]
    )
    if (!identical(newStoredString, storedString)) return(newStoredString)
  }
}

evaluateMatrix <- function(cell) {
  if (notApplicable(cell) || is.numeric(cell) || !grepl(":", cell)) return(cell)
  if (is.factor(cell)) {
    return(as.numeric(cell))
  }
  # evaluateCell entire matrix, subsetting and returning it back isn't working
  proto_matrix <- sub(
    "\\[{1}\\d+\\:?\\d*\\,{1}\\d+\\:?\\d*\\]{1}",
    "",
    cell
  )
  proto_matrix <- gsub("%", "", proto_matrix)
  currentLattitude <- as.data.frame(
    apply(
      eval(parse(text=proto_matrix)),
      1:2,
      computeStandardR
    ), stringsAsFactors=F
  )
  currentLattitude <- as.data.frame(
    lapply(
      currentLattitude,
      function(x) as.numeric(as.character(x))
    ), stringsAsFactors=F
  )
  currentLattitudeColumns <- colnames(currentLattitude)
  colnames(currentLattitude) <- currentLattitudeColumns
  if (grepl("%", cell)) {
    currentLattitude <- as.matrix(currentLattitude)
  }
  proto_matrix <- gsub("`", "", proto_matrix)
  eval(call("<<-",
    as.name(proto_matrix),
    currentLattitude
  ))
}

splitAndReplaceMatrix <- function(cell) {
  if (notApplicable(cell) || is.numeric(cell)) return(cell)
  if (is.factor(cell)) {
    return(as.numeric(cell))
  }
  stringParts <- strsplit(
    cell,
    EQUATION_SPLIT
  )[[1]]
  lapply(stringParts, evaluateMatrix)
  return(splitAndReplace(evaluateCell(cell)))
}

applyNumeric <- function(slide) {
  apply(
    eval(parse(text=paste0("`", slide, "`"))),
    1:2,
    function(x) as.numeric(as.character(x))
  )
}

configureSheetsToMatrix <- function(slide) {
  longitude <- matrix(
    unlist(slide),
    ncol=length(slide[[1]][[1]]),
    byrow=T
  )
  colnames(longitude) <- longitude[1,]
  longitude <- longitude[-1,]
  return(longitude)
}

isCircularReference <- function(cell) {
  errortype <- '#ERROR!'
  for (sheet in sheetNamesNoRewrite) {
    circularReference <- paste0(
			"\\`?(?<=",
			sheet,
			")\\`?\\[{1}\\d+\\,{1}\\d+\\]{1}"
		)
    if (grepl(circularReference, cell, perl=T)) {
      errortype <- '#REF!'
    }
  }
  return(toJSON(errortype))
}

#' parse cell
#' @param slides worksheet data
#' @param cell the cell we want to parse
#' @param names array of sheet names
#' @post /cloudR
function(slides, cell, names) {
	lattitude <- fromJSON(slides, simplifyMatrix=F)
	sheetNamesNoRewrite <<- fromJSON(names)

	for (slide in 1:length(lattitude)) {
		longitude <- configureSheetsToMatrix(lattitude[slide])
		eval(call("<<-", as.name(sheetNamesNoRewrite[slide]), longitude))
	}

	# Matrix multiplication expected to be numeric so cell should not be evaluated prior to applyNumeric
  # Should not evaluateCell prior to splitAndReplace, only first element will be used
	storedString <- cell
	if (!grepl("%*%", cell)) {
		while(!identical(storedString, splitAndReplace(storedString))) {
			storedString <- splitAndReplace(storedString)
      print(storedString)
		}
	}
	# "Sheet1!C3 -- Sheet1!C4 -- Dec-31 or 0.42 or 1+3"
	# "Sheet1!C3 -- Sheet1!C4 + Sheet1!C5 -- 0.43"
  if (is.numeric(storedString)) {
		jsonString <- toJSON(storedString, digits=NA)
	} else if (identical("", storedString)) {
    jsonString <- toJSON("")
  } else {
		# "Sheet1!C3 + Sheet1!C6 -- 42 or Dec-31+0.1919"
		# "Sheet1!C3 -- Dec-31"
		for (slide in 1:length(sheetNamesNoRewrite)) {
			# "Sheet1!C3 -- mean(Sheet1$Rating_X) + 0.43"
			if (grepl("$", storedString, fixed=T)) {
				eval(call("<<-",
					as.name(sheetNamesNoRewrite[slide]),
					as.data.frame(
						applyNumeric(sheetNamesNoRewrite[slide]
					), stringsAsFactors=F)
				))
			# "Sheet1!C3 -- mean(Sheet1[,2:3]) + 0.43"
			} else {
				eval(call("<<-",
					as.name(sheetNamesNoRewrite[slide]),
					applyNumeric(sheetNamesNoRewrite[slide])
				))
			}
			newStoredString <- splitAndReplace(storedString)
			if (identical(newStoredString, storedString)) {
				jsonString <- isCircularReference(newStoredString)
			} else {
				jsonString <- toJSON(
					newStoredString,
					digits=NA)
			}
		}
	}

	# "mean(Sheet1[2:3,2:2]) + mean(Sheet1[3:4,2:2]) -- mean(Sheet2[,2:2]), mean(Sheet2[,3:3])"
	# "Sheet4[1:3,1:3] %*% Sheet4[1:3,2:4]"
	if ((identical(jsonString, toJSON('#ERROR!')) || identical(jsonString, toJSON('#REF!'))) && grepl(":", storedString)) {
		# Restart Sheet calculations because previous one has transformed to NA
    for (slide in 1:length(lattitude)) {
      longitude <- configureSheetsToMatrix(lattitude[slide])
      eval(call("<<-", as.name(sheetNamesNoRewrite[slide]), longitude))
    }
		newStoredString <- splitAndReplaceMatrix(storedString)
		if (identical(newStoredString, storedString)) {
			jsonString <- isCircularReference(newStoredString)
		} else {
			jsonString <- toJSON(newStoredString, digits=NA)
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
  lattitude <- fromJSON(slides, simplifyMatrix=F)
	sheetNamesNoRewrite <<- fromJSON(names)

  for (slide in 1:length(lattitude)) {
		longitude <- configureSheetsToMatrix(lattitude[slide])
		eval(call("<<-", as.name(sheetNamesNoRewrite[slide]), longitude))
	}

  currentSlide <- eval(parse(text=paste0("`", name, "`")))
	currentLattitude <- as.data.frame(apply(
    currentSlide, 1:2, computeStandardR), stringsAsFactors=F)
	currentLattitudeColumns <- colnames(currentLattitude)
  currentLattitude <- as.data.frame(lapply(
		currentLattitude, function(x) as.numeric(as.character(x))), stringsAsFactors=F)
  colnames(currentLattitude) <- currentLattitudeColumns

  for (slide in 1:length(lattitude)) {
		longitude <- configureSheetsToMatrix(lattitude[slide])
		eval(call("<<-", as.name(sheetNamesNoRewrite[slide]), longitude))
	}

  # Compare currentLattitude with unaltered longitude and try calculate missing values
  for (row in 1:nrow(currentLattitude)) {
  	for (column in 1:ncol(currentLattitude)) {
  		rawCell <- currentSlide[row,column]
    	cell <- currentLattitude[row,column]
      if (notApplicable(rawCell) || !is.na(cell)) next
  		storedString <- splitAndReplace(evaluateCell(rawCell))
  		newCell <- as.numeric(as.character(splitAndReplaceMatrix(storedString)))
  		if (length(newCell) > 1) {
  			currentLattitude[row,column] <- newCell[1]
  		} else {
  			currentLattitude[row,column] <- newCell
  		}
  	}
  }

	# PLOT
  if (hasArg(variabley)) {
  	pngChart <- ggplot(
  		currentLattitude,
  		aes_string(
  			paste0("`", variablex, "`"),
  			paste0("`", variabley, "`")
      )
    ) + eval(parse(text=type))
  } else {
  	pngChart <- ggplot(
  		currentLattitude,
  		aes_string(paste0("`", variablex, "`"))
    ) + eval(parse(text=type))
  }
  print(pngChart)
}

#' Analyze data
#' @param slides worksheet data
#' @param names array of sheet names
#' @param formulatext formula
#' @param name current sheet name
#' @post /regression
function(slides, names, formulatext, name) {
  lattitude <- fromJSON(slides, simplifyMatrix=F)
	sheetNamesNoRewrite <<- fromJSON(names)

  for (slide in 1:length(lattitude)) {
		longitude <- configureSheetsToMatrix(lattitude[slide])
		eval(call("<<-", as.name(sheetNamesNoRewrite[slide]), longitude))
	}

  currentSlide <- eval(parse(text=paste0("`", name, "`")))
	currentLattitude <- as.data.frame(apply(
    currentSlide, 1:2, computeStandardR), stringsAsFactors=F)
	currentLattitudeColumns <- colnames(currentLattitude)
  currentLattitude <- as.data.frame(lapply(
		currentLattitude, function(x) as.numeric(as.character(x))), stringsAsFactors=F)
  colnames(currentLattitude) <- currentLattitudeColumns

  for (slide in 1:length(lattitude)) {
		longitude <- configureSheetsToMatrix(lattitude[slide])
		eval(call("<<-", as.name(sheetNamesNoRewrite[slide]), longitude))
	}

  # Compare currentLattitude with unaltered longitude and try calculate missing values
  for (row in 1:nrow(currentLattitude)) {
  	for (column in 1:ncol(currentLattitude)) {
  		rawCell <- currentSlide[row,column]
    	cell <- currentLattitude[row,column]
      if (notApplicable(rawCell) || !is.na(cell)) next
  		storedString <- splitAndReplace(evaluateCell(rawCell))
  		newCell <- as.numeric(as.character(splitAndReplaceMatrix(storedString)))
  		if (length(newCell) > 1) {
  			currentLattitude[row,column] <- newCell[1]
  		} else {
  			currentLattitude[row,column] <- newCell
  		}
  	}
  }

	# REGRESS
	regressionParsed <- tryCatch({eval(parse(text = formulatext))},
    error = function(cond) {return(toString(cond))})
	jsonString <- toJSON(regressionParsed)
	jsonString
}

RANGE_REGEX <- "\\[{1}\\d+\\:?\\d*\\,{1}\\d+\\:?\\d*\\]{1}"

appendMatchNumbers <- function(matchNumbers, matchRange) {
  if (length(matchNumbers) < 4) {
    if (grepl("\\:{1}\\d+\\,{1}", matchRange)) {
      return(append(matchNumbers, matchNumbers[3], after=3))
    } else if (grepl("\\,{1}\\d+\\:{1}", matchRange)) {
      return(append(matchNumbers, matchNumbers[1], after=0))
    }
  }
  return(matchNumbers)
}

isValidParameters <- function(range) {
  matchRange <- regmatches(range, regexpr(RANGE_REGEX, range))
  if (length(matchRange) < 1) return(F)
  # Tests for single cell [8,2]
  if(grepl("\\[{1}\\d+\\,{1}\\d+\\]{1}", matchRange)) return(T)
  matchNumbers <- regmatches(matchRange, gregexpr("\\d+", matchRange))[[1]]
  # Tests [8:8,2] or [8,2:3]
  matchNumbers <- appendMatchNumbers(matchNumbers, matchRange)
  if (length(matchNumbers) != 4) return(F)
  return(T)
}

createListFromRange <- function(range) {
	if (length(grep(":", range)) < 1) return(range)
	matchRange <- regmatches(range, regexpr(RANGE_REGEX, range))
	matchSlide <- sub(RANGE_REGEX, "", range)
	matchNumbers <- regmatches(matchRange, gregexpr("\\d+", matchRange))[[1]]
	# Tests [8:8,2] or [8,2:3]
  matchNumbers <- as.numeric(appendMatchNumbers(matchNumbers, matchRange))
	# totalRows <- matchNumbers[2] - matchNumbers[1] + 1
	# totalColumns  <- matchNumbers[4] - matchNumbers[3] + 3
	resultList <- c()
	for (row in matchNumbers[1]:matchNumbers[2]) {
		for (column in matchNumbers[3]:matchNumbers[4]) {
			cell <- paste0(matchSlide, "[", row, ",", column, "]")
			resultList <- append(resultList, cell)
		}
	}
	return(resultList)
}

createListFromRangeOnlyQuotes <- function(range) {
  matchRange <- regmatches(range, regexpr(RANGE_REGEX, range))
  matchSlide <- sub(RANGE_REGEX, "", range)
  matchNumbers <- regmatches(matchRange, gregexpr("\\d+", matchRange))[[1]]
  # Tests for single cell [8,2]
  if (length(grep(":", range)) < 1) {
    return(paste0("`", matchSlide, "`[", matchNumbers[1], ",", matchNumbers[2], "]"))
  }
  # Tests [8:8,2] or [8,2:3]
  matchNumbers <- as.numeric(appendMatchNumbers(matchNumbers, matchRange))
  resultList <- c()
  for (row in matchNumbers[1]:matchNumbers[2]) {
    for (column in matchNumbers[3]:matchNumbers[4]) {
      cell <- paste0("`", matchSlide, "`[", row, ",", column, "]")
      resultList <- append(resultList, cell)
    }
  }
  return(resultList)
}

createListFromRangeWithQuotes <- function(range) {
  matchRange <- regmatches(range, regexpr(RANGE_REGEX, range))
  matchSlide <- sub(RANGE_REGEX, "", range)
  matchNumbers <- regmatches(matchRange, gregexpr("\\d+", matchRange))[[1]]
  # Tests for single cell [8,2]
  if (length(grep(":", range)) < 1) {
    return(append(range, paste0("`", matchSlide, "`[", matchNumbers[1], ",", matchNumbers[2], "]")))
  }
  # Tests [8:8,2] or [8,2:3]
  matchNumbers <- as.numeric(appendMatchNumbers(matchNumbers, matchRange))
  resultList <- c()
  for (row in matchNumbers[1]:matchNumbers[2]) {
    for (column in matchNumbers[3]:matchNumbers[4]) {
      cell <- paste0("`", matchSlide, "`[", row, ",", column, "]")
      resultList <- append(resultList, cell)
    }
  }
  return(resultList)
}


evaluateCellExceptDecision <- function(cell) {
	if (notApplicable(cell) || cell %in% parametersListWithQuotes) return(cell)
	result <- cell
	try(
		while(!identical(result,eval(parse(text=result)))) {
			if (is.na(eval(parse(text=result))) || result %in% parametersListWithQuotes) break
			result <- eval(parse(text=result))
			if (length(result) > 1) break
		}, silent=T
	)
	if(is.null(result) || is.function(result)) return(cell)
	return(result)
}

EQUATION_FORM_SPLIT <- "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\(|\\)|\\s|(?<=\\])\\s*\\,"

splitAndReplaceForm <- function(cell) {
  if (notApplicable(cell) || is.numeric(cell)) return(cell)
  if (is.factor(cell)) {
    return(as.numeric(cell))
  }
  stringParts <- strsplit(cell, EQUATION_FORM_SPLIT, perl=T)[[1]]
  evaluateParts <- sapply(
    stringParts,
    evaluateCellExceptDecision,
    USE.NAMES=F
  )
  newCell <- cell
  for (part in 1:length(stringParts)) {
    if(!identical(stringParts[part],"")) {
      negativeString <- paste0("-", stringParts[part])
      matchNegative <- grep(negativeString, newCell, fixed=T)
      if (length(matchNegative) > 0) {
        negativeEvaluate <- paste0("(", evaluateParts[part], ")")
        newCell <- sub(
          stringParts[part],
          negativeEvaluate,
          newCell,
          fixed=T
        )
      } else {
        newCell <- sub(
          stringParts[part],
          evaluateParts[part],
          newCell,
          fixed=T
        )
      }
    }
  }
  # replace parameters with x[1], x[2]
  for (par in 1:length(parametersList)) {
    newCell <- gsub(
      parametersList[par],
      paste0("x[",par,"]"),
      newCell,
      fixed=T
    )
    newCell <- gsub(
      parametersListOnlyQuotes[par],
      paste0("x[",par,"]"),
      newCell,
      fixed=T
    )
  }
  return(newCell)
}

evaluateCellNoRewriteExceptDecision <- function(cell, actualData, sheetname) {
  if (notApplicable(cell) || cell %in% parametersListWithQuotes) return(cell)
  if (exists(sheetname)) {
    eval(call("<-", as.name(sheetname), actualData))
  }
  result <- cell
  try(
    while(!identical(result,eval(parse(text=result)))) {
      if (is.na(eval(parse(text=result))) || result %in% parametersListWithQuotes) break
      result <- eval(parse(text=result))
      if (length(result) > 1) break
    }, silent=T
  )
  if (is.null(result) || is.function(result)) return(cell)
  return(result)
}

splitAndReplaceNoRewriteForm <- function(cell, actualData, sheetname) {
  if (notApplicable(cell) || is.numeric(cell)) return(cell)
  if (is.factor(cell)) {
    return(as.numeric(cell))
  }
  stringParts <- strsplit(cell, "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\(|\\)|\\s")[[1]]
  evaluateParts <- simplify2array(lapply(
    stringParts,
    evaluateCellNoRewriteExceptDecision,
    actualData=actualData,
    sheetname=sheetname
  ))
  newCell <- cell
  for (part in 1:length(stringParts)) {
    if(!identical(stringParts[part],"")) {
      newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed=T)
    }
  }
  # "mean(Sheet4[,2:2])"
  stringParts <- strsplit(newCell, "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\s")[[1]]
  evaluateParts <- simplify2array(lapply(
    stringParts,
    evaluateCellNoRewriteExceptDecision,
    actualData=actualData,
    sheetname=sheetname
  ))
  for (part in 1:length(stringParts)) {
    if(!identical(stringParts[part],"") && grepl("(", stringParts[part], fixed=T) && grepl(")", stringParts[part], fixed=T)) {
      newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed=T)
    }
  }
  return(newCell)
}

createGeneralFormFunction <- function(cell) {
  form <- evaluateCellExceptDecision(cell)
  while(!identical(form, splitAndReplaceForm(form))) {
    form <- splitAndReplaceForm(form)
  }
  for (slide in 1:length(sheetNamesNoRewrite)) {
    # "Sheet1!C3 -- mean(Sheet1$Rating_X) + 0.43"
    if (grepl("$", form, fixed=T)) {
      eval(call("<-",
        as.name(sheetNamesNoRewrite[slide]),
        as.data.frame(applyNumeric(sheetNamesNoRewrite[slide]), stringsAsFactors=F)
      ))
    # "Sheet1!C3 -- mean(Sheet1[,2:3]) + 0.43"
    } else {
      eval(call("<-",
        as.name(sheetNamesNoRewrite[slide]),
        applyNumeric(sheetNamesNoRewrite[slide])
      ))
    }
    sheetnameAsString <- paste0("`", sheetNamesNoRewrite[slide], "`")
    newform <- splitAndReplaceNoRewriteForm(
      form,
      eval(parse(text=sheetnameAsString)),
      sheetNamesNoRewrite[slide]
    )
    if (!identical(newform, form)) {
      form <- newform
    }
  }
  return(form)
}

isValidObjective <- function(formula) {
  isValid <- F
  for (par in 1:length(parametersList)) {
    if (grepl(paste0("x[", par, "]"), formula, fixed=T)) {
      isValid <- T
    }
  }
  return(isValid)
}

createQuadraticObjective <- function(objective, quadraticLinearObjective) {
  Q0 <- evaluateCell(objective)
  Q0 <- apply(Q0, 1:2, computeStandardR)
  if (identical(quadraticLinearObjective, 'na')) {
    Qobj <- Q_objective(Q=Q0)
  } else {
    L0 <- evaluateCell(quadraticLinearObjective)
    L0 <- apply(L0, 1:2, computeStandardR)
    Qobj <- Q_objective(Q=Q0, L=L0)
  }
  return(Qobj)
}

matchLinearCoefficient <- function(cell) {
  coef <- c()
  for (par in 1:length(parametersList)) {
    linearCoefficientRegex <- paste0("\\-?\\d*\\*?\\(?[x]{1}\\[", par, "]{1}(?!\\^)\\)?")
    linearMatch <- regmatches(cell, regexpr(linearCoefficientRegex, cell, perl=T))
    linearCoefficient <- regmatches(linearMatch, regexpr("\\-?\\d*", linearMatch))
    if (linearCoefficient == "-") {
      linearCoefficient <- -1
    } else if (linearCoefficient == "") {
      linearCoefficient <- 1
    } else {
      linearCoefficient <- as.numeric(linearCoefficient)
    }
    coef <- append(coef, linearCoefficient)
  }
  return(coef)
}

createLinearObjective <- function(objectiveFormula) {
  L_objective(
    matchLinearCoefficient(objectiveFormula),
    names=parametersList
  )
}

createGeneralFormObjective <- function(objectiveFormula) {
  # Can't do sum(Sheet4[,2:2]*3) -- sum(Sheet4[1,2]*3, Sheet4[2,2]*3...)
  objectiveFunction <- paste0("function(x){", objectiveFormula, "}")
  objectiveFunction <- eval(parse(text=objectiveFunction))
  F_objective(objectiveFunction, length(parametersList), names=parametersList)
}

createListAndEvaluate <- function(stringList) {
  objectList <- createListFromRange(stringList)
  sapply(objectList, createGeneralFormFunction, USE.NAMES=F)
}

checkDirection <- function(x) {
  if (identical(x, "<=") || identical(x, "=") || identical(x, ">=")) {
    return(T)
  } else {
    return(F)
  }
}

checkIsValidConstraint <- function(formula) {
  isValid <- F
  for (par in 1:length(parametersList)) {
    if (grepl(paste0("x[", par, "]"), formula, fixed=T)) {
      isValid <- T
    }
  }
  return(isValid)
}

createEqualBound <- function(indice, rhs) {
  bound <- V_bound(
    li=indice,
    ui=indice,
    lb=rhs,
    ub=rhs,
    nobj=length(parametersList),
    names=parametersList
  )
  rbind(bounds(optimization), bound)
}

createGreaterThanOrEqualBound <- function(indice, rhs) {
  bound <- V_bound(
    li=indice,
    ui=indice,
    lb=rhs,
    nobj=length(parametersList),
    names=parametersList
  )
  rbind(bounds(optimization), bound)
}

createLessThanOrEqualBound <- function(indice, rhs) {
  bound <- V_bound(
    li=indice,
    ui=indice,
    ub=rhs,
    nobj=length(parametersList),
    names=parametersList
  )
  rbind(bounds(optimization), bound)
}

createLinearConstraint <- function(lhs, dir, rhs) {
  if (dir == "=") {
    dir <- "=="
  }
  linearConstraint <- L_constraint(
    L=matchLinearCoefficient(lhs),
    dir=dir,
    rhs=rhs,
    names=parametersList
  )
  rbind(constraints(optimization), linearConstraint)
}

createGeneralFormConstraint <- function(lhs, dir, rhs) {
  if (dir == "=") {
    dir <- "=="
  }
  functionConstraint <- eval(parse(text=paste0("function(x){", lhs, "}")))
  functionConstraint <- F_constraint(
    F=functionConstraint,
    dir=dir,
    rhs=rhs,
    names=parametersList
  )
  rbind(constraints(optimization), functionConstraint)
}

convertParametersAsData <- function(parametersList) {
  namesList <- parametersList
  for (par in 1:length(parametersList)) {
    parametersList[par] <- evaluateCell(parametersList[par])
    namesList[par] <- paste0("x", par)
  }
  parametersList <- as.numeric(parametersList)
  names(parametersList) <- namesList
  return(parametersList)
}

#' Optimization
#' @param slides worksheet data
#' @param names array of sheet names
#' @param objective cell we want to optimize
#' @param qudaraticLinearObjective linear portion of a quadratic Objective
#' @param objectiveClass use general nonlinear, linear or qudaratic
#' @param isMaximum whether to maximize or minimize objective
#' @param parametersString decision variables to Manipulate
#' @param constraintsLhs constraints left hand side
#' @param constraintsDir constraints inequality direction
#' @param constraintsRhs constraints right hand side
#' @post /optimization
function(slides, names, objective, quadraticLinearObjective, objectiveClass, isMaximum, parametersString, constraintsLhs, constraintsDir, constraintsRhs) {
	optimization <- OP()
  lattitude <- fromJSON(slides, simplifyMatrix=F)
	sheetNamesNoRewrite <<- fromJSON(names)

	nloptrComparison <- function() {
		applicable <- ROI_applicable_solvers(optimization)
		applicable <- applicable[grepl("nloptr", applicable)]
		if (length(applicable) < 1) {
			return("Optimal Solution not found.")
		}
		selectedSolver <- 1
		optimalValue <- tryCatch({
			ROI_solve(
				optimization,
				applicable[selectedSolver],
				start=parametersStart)
		}, error=function(cond) return("error"))
		while(identical(optimalValue, "error") && selectedSolver < length(applicable)) {
			selectedSolver <- selectedSolver + 1
			optimalValue <- tryCatch({
				ROI_solve(
					optimization,
					applicable[selectedsolver],
					start=parametersStart)
			}, error=function(cond) return("error"))
		}
		return(optimalValue)
	}

	# checkIsValidQuadratic <- function() {
	# 	isValid <- F
	# 	quadraticTerms <- length(gregexpr(
	# 		"\\^{1}",
	# 		objective
	# 	)[[1]])
	#
	# 	if (quadraticTerms != )
	# }

	nested_unclass <- function(x) {
		x <- unclass(x)
		if (is.list(x)) {
			x <- lapply(x, nested_unclass)
		}
		return(x)
	}

	# A1
	# Sheet1[1,1]
	# C12 + D12
	# C3 * C9 - (C2 * C7 + C8) + C3 * D9 - (C2 * D7 + D8)
	# 100 * MIN(C8/20, 400) - 20000 - C8 + 100 * MIN(D8/20, 600) - 30000 - D8

	# Slides
  for (slide in 1:length(lattitude)) {
		longitude <- configureSheetsToMatrix(lattitude[slide])
		eval(call("<<-", as.name(sheetNamesNoRewrite[slide]), longitude))
	}

  # Parameters
  isObjectiveQuadratic <- identical(objectiveClass, "Quadratic programming")
  isObjectiveLinear <- identical(objectiveClass, "Linear programming")
  if (!isObjectiveQuadratic || !isObjectiveLinear || !isValidParameters(parametersString)) {
    stop("Invalid decision variables.")
  } else {
    parametersList <<- createListFromRange(parametersString)
    parametersListWithQuotes <<- createListFromRangeWithQuotes(parametersString)
    parametersListOnlyQuotes <<- createListFromRangeOnlyQuotes(parametersString)
  }

  # Objective
  objectiveFormula <- createGeneralFormFunction(objective)
  if (!isValidObjective(objectiveFormula)) {
    stop("A decision variable(s) is missing from the objective.")
  }

  if (isObjectiveQuadratic && length(evaluateCell(objective)) < 4) {
    stop("Invalid range for quadratic portion of the objective.")
  }

  if (isObjectiveQuadratic) {
    objective(optimization) <- createQuadraticObjective(objective, quadraticLinearObjective)
  } else if (identical(objectiveClass, "Linear programming")) {
		objective(optimization) <- createLinearObjective(objectiveFormula)
	} else if (identical(objectiveClass, "General nonlinear optimization")) {
		objective(optimization) <- createGeneralFormObjective(objectiveFormula)
	}

  # Constraints direction
  constraintsDirList <- createListAndEvaluate(constraintsDir)
  if (identical(constraintsDir, "na") || F %in% sapply(constraintsDirList, checkDirection, USE.NAMES=F)) {
    stop("Direction of constraints must either be =, <= or >=")
  }

	# Constraints right hand side
  constraintsRhsList <- createListAndEvaluate(constraintsRhs)
  if (identical(constraintsRhs, "na") || !is.numeric(constraintsRhsList)) {
    stop("Right hand side constraints must be numeric.")
  }

  # Constraints left hand side
  constraintsLhsList <- createListAndEvaluate(constraintsLhs)
  if (identical(constraintsLhs, "na") ||  F %in% sapply(constraintsLhsList, checkIsValidConstraint, USE.NAMES=F)) {
    stop("Constraints must contain a decision variable")
  }

	onlyLinearConstraints <- T
	onlyBound <- T
	for (con in 1:length(constraintsLhsList)) {
		splitLhs <- strsplit(constraintsLhsList[con], "\\+|\\-|\\^|\\%%|\\%/%")[[1]]
    hasMatchLhs <- gregexpr("\\-?\\d*\\*?\\(?[x]{1}\\[{1}\\d+\\]{1}\\)?", constraintsLhsList[con])
		matchLhs <- regmatches(constraintsLhsList[con], hasMatchLhs)[[1]]
		if (!identical(splitLhs, matchLhs)) {
			onlyLinearConstraints <- F
			if (length(splitLhs) != 1) {
				onlyBound <- F
			}
		}

		### Yet to implement: Quadratic and Conic Optimization
		if (identical(splitLhs, matchLhs)) {
			if (length(splitLhs) == 1) {
				# Bounds
				if (identical(constraintsDirList[con], ">=") && !identical(constraintsRhsList[con],0)) {
					indice <- regmatches(constraintsLhsList[con], regexpr("\\d+", constraintsLhsList[con]))
					if (identical(constraintsDirList[con], "=")) {
						bounds(optimization) <- createEqualBound(indice, constraintsRhsList[con])
					} else if (identical(constraintsDirList[con], ">=")) {
						bounds(optimization) <- createGreaterThanOrEqualBound(indice, constraintsRhsList[con])
					} else if (identical(constraintsDirList[con], "<=")) {
						bounds(optimization) <- createLessThanOrEqualBound(indice, constraintsRhsList[con])
					}
				}
			} else {
				# Linear constraint
				constraints(optimization) <- createLinearConstraint(
					constraintsLhsList[con],
					constraintsDirList[con],
					constraintsRhsList[con]
        )
			}
		} else {
			# General from constraint
			constraints(optimization) <- createGeneralFormConstraint(
				constraintsLhsList[con],
				constraintsDirList[con],
				constraintsRhsList[con]
      )
		}
	}

	if (identical(isMaximum, "maximum")) {
		maximum(optimization) <- T
	}

  parametersStart <- convertParametersAsData(parametersList)
	if (anyNA(parametersStart)) {
		stop("Right hand side constraints must be numeric.")
	}

		if (identical(constraintsLhs, "na")) {
			if (identical(objectiveClass, "General nonlinear optimization")) {
				solution <- tryCatch({
					ROI_solve(optimization, "optimx", start=parametersStart)
				}, error=function(cond) {
					return("Optimial solution not found.")
				})
			} else {
				solution <- "Constraints must be linear for linear and quadratic objectives."
			}
		} else if (onlyLinearConstraints) {
			if (identical(objectiveClass, "General nonlinear optimization") && onlyBound) {
				solution <- tryCatch({
					ROI_solve(optimization, "optimx", start=parametersStart)
				}, error=function(cond) return("No solution found."))
			} else if (identical(objectiveClass, "Quadratic programming")) {
				solution <- ROI_solve(optimization, "quadprog", start=parametersStart)
				# Non-convex quadratic with Linear constraint use qpoases
			} else if (identical(objectiveClass, "Linear programming")) {
				solution <- ROI_solve(optimization, "lpsolve", start=parametersStart)
			} else {
				solution <- ROI_solve(optimization, "nloptr.neldermead", start=parametersStart)
			}
		} else {
			# General form constraints
			solution <- nloptrComparison()
		}
		solution <- nested_unclass(solution)
		result <- matrix(c(solution$solution, solution$objval, solution$status$msg$solver), nrow=1, byrow=T)
		colnames(result) <- c(names(solution$solution), 'objective', 'method')
		jsonString <- toJSON(as_tibble(result))
	}
	jsonString
}

#
# sparklyr.R
#
# Created by Edbert Dudon on 7/8/19.
# Copyright © 2019 Project Tart. All rights reserved.
#
library(googleCloudStorageR)
library(SparkR)
library(sparklyr)
library(googleAuthR)
library(rdatastore)
# jsonlite
library(slam)
library(ggplot2)
# dplyr
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

#
## Function Required for Computation
#

notApplicable <- function(cell) {
  is.null(cell) || is.na(cell) || cell=='' || !is.character(cell)
}

LETTERS_REFERENCE <- letters[1:26]

lettersToColumn <- function(letter) {
	column <- 0
	letterlength <- nchar(letter)
	for (i in 1:letterlength) {
		column <- match(tolower(letter), LETTERS_REFERENCE) * 26^(letterlength-i) + column
	}
	return(column)
}

FUNCTIONS_WITH_PREFIX <- c('rnorm(', 'rexp(', 'rgamma(', 'rpois(', 'rweibull(', 'rcauchy(', 'rbeta(', 'rt(', 'rf(',
	'rbinom(', 'rgeom(', 'rhyper(', 'rlogis(', 'rlnorm(', 'rnbinom(', 'runif(', 'rwilcox(')

addPrefixToFunction <- function(cell) {
	for (f in FUNCTIONS_WITH_PREFIX) {
		cell <- gsub(f, paste0(f, '1,'), cell, fixed=T)
	}
	return(cell)
}

translateRForProcess <- function(cell, currentSlide) {
	if (notApplicable(cell) || !identical(substring(cell,1,1),'=')) return(cell)
	match <- regmatches(cell, gregexpr('[[:upper:]]+\\d*', cell))[[1]]
	coordinates <- gsub("'", '`', substring(cell, 2), fixed=T)
	for (i in 1:length(initialDataNoRewrite)) {
		coordinates <- gsub(rawSheetNamesNoRewrite[i], sheetNamesNoRewrite[i], coordinates)
	}
	if (length(match) == 0) return(coordinates)
	for (i in 1:length(match)) {
    column <- lettersToColumn(regmatches(match[i], regexpr('[[:upper:]]', match[i])))
    row <- as.numeric(regmatches(match[i], regexpr('\\d+', match[i])))
		if (!identical(i, length(match))) {
			column2 <- lettersToColumn(regmatches(match[i+1], regexpr('[[:upper:]]', match[i+1])))
			row2 <- as.numeric(regmatches(match[i+1], regexpr('\\d+', match[i+1])))
			if (!grepl('\\d', match[i])) {
				ref5 <- paste0('[,', column, ':', column2, ']')
				prefix5 <- paste0('!', match[i], ':', match[i+1])
				coordinates <- gsub(prefix5, ref5, coordinates)
				ref6 <- paste0('`', sheetNamesNoRewrite[currentSlide], '`[,', column, ':', column2, ']')
				prefix6 <- paste0(match[i], ':', match[i+1])
				coordinates <- gsub(prefix6, ref6, coordinates)
			}
			ref4 <- paste0('[', row, ':', row2, ',', column, ':', column2, ']')
			prefix4 <- paste0('!', match[i], ':', match[i+1])
			coordinates <- gsub(prefix4, ref4,coordinates)
			ref3 <- paste0('`', sheetNamesNoRewrite[currentSlide], '`[', row, ':', row2, ',', column, ':', column2, ']')
			prefix3 <- paste0(match[i], ':', match[i+1])
			coordinates <- gsub(prefix3, ref3, coordinates)
		}
		if (grepl('\\d', match[i])) {
			ref <- paste0('[', row, ',', column, ']')
			prefix <- paste0('!', match[i])
			coordinates <- gsub(prefix, ref, coordinates)
			ref2 <- paste0('`', sheetNamesNoRewrite[currentSlide], '`[', row, ',', column, ']')
			prefix2 <- match[i]
			coordinates <- gsub(prefix2, ref2, coordinates)
		}
	}
	return(addPrefixToFunction(coordinates))
}

configureSheetsToMatrixNoRewrite <- function() {
  for (s in 1:length(sheetNamesNoRewrite)) {
    currentLattitude <- initialDataNoRewrite[[s]]$rows
    currentLattitude <- currentLattitude[names(currentLattitude) != 'len']
    rows <- as.numeric(names(tail(currentLattitude, n=1)))+1
    columns <- 1
    for (row in currentLattitude) {
      n <- max(as.numeric(names(row$cells)))
      if (n > columns) {
        columns <- n
      }
    }
    columns <- columns + 1
    df <- matrix(NA, rows, columns)
    for (r in 1:rows) {
      for (c in 1:columns) {
        value <- currentLattitude[[toString(r-1)]]$cells[[toString(c-1)]]$text
        if (!is.null(value)) {
          df[r, c] <- value
        }
      }
    }
    currentLattitude <- apply(df, 1:2, function(x) translateRForProcess(x, s))
    eval(call('<<-', as.name(sheetNamesNoRewrite[s]), currentLattitude))
  }
}

evalParse <- function(cell) {
  eval(parse(text=cell))
}

FULL_COLUMN_ROW <- '\\[{1},{1}\\d+\\:?\\d*\\]{1}|\\[{1}\\d+\\:?\\d*\\,{1}\\]{1}'

evaluateCellNoRewrite <- function(cell) {
  if (notApplicable(cell)) return(cell)
  result <- cell
  try(
    # cell != NA returns NA instead of TRUE/FALSE
    while(!identical(result, evalParse(result))) {
      proto_name <- sheetNamesNoRewrite
      for (n in sheetNamesNoRewrite) {
        if (!grepl(n, result)) {
          proto_name <- proto_name[proto_name != n]
        }
      }
      # sparklyr must contain only one input slide
      if (length(proto_name) == 1 && any(grepl(proto_name, inputNamesNoRewrite))) {
        proto_cell <- gsub(matchedSheetNames[name], 'e', cell)
        pickup_dropoff <- tryCatch({
          evalParse(proto_name) %>%
            spark_apply(function(e) evalParse(proto_cell)) %>%
            collect()
        }, error=function(c) NA)
        if (length(pickup_dropoff) == 1) {
          nextResult <- pickup_dropoff
        }
      # Prevent Statistics/Optimizations cell references from being calculated
      } else if (any(grepl(proto_name, statOptimNamesNoRewrite))) {
        nextResult <- result
      # mean(test.csv[,1:1]) not mean(sheet1[,1:1]) (cell referenced: 'sheet1[2:2]')
      } else if (grepl(FULL_COLUMN_ROW, result)
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
    },
  silent=T)
  if(is.null(result) || is.function(result)) return(cell)
  return(result)
}

EQUATION_FORM_SPLIT <- '\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|(?!\\d),(?!\\d)|\\s|(?<=\\])\\s*\\,|\\,(?=\\()|(?<=\\))\\,|\\((?=\\()|(?<=\\))\\)'

splitAndReplaceNoRewrite <- function(cell, dataname) {
  # matrix(c(1:9), 3), sheet1[1:10,1] for length(cell) > 1
  cell <- evaluateCellNoRewrite(cell)
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
  evaluateParts <- sapply(stringParts, evaluateCellNoRewrite, USE.NAMES=F)
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
  evaluateCellNoRewrite(newCell)
}

hasCellReferenceNoRewrite <- function(cell) {
  for (sheet in sheetNamesNoRewrite) {
    if (grepl(sheet, cell)) {
      return(T)
    }
  }
  return(F)
}

applyNumeric <- function(slide) {
  slide <- evalParse(paste0('`', slide, '`'))
  apply(slide, 1:2, function(x) as.numeric(as.character(x)))
}

numerizeAndSplitNoRewrite <- function(cell, original) {
  if (notApplicable(cell)) return(cell)
  # 'Sheet1!C3 -- Sheet1!C4 -- Dec-31 or 0.42 or 1+3'
  # 'Sheet1!C3 -- Sheet1!C4 + Sheet1!C5 -- 0.43'
  if (is.numeric(cell) || !hasCellReferenceNoRewrite(cell)) {
    return(cell)
  }
  # 'Sheet1!C3 + Sheet1!C6 -- 42 or Dec-31+0.1919'
  # 'Sheet1!C3 -- Dec-31'
  for (slide in 1:length(sheetNamesNoRewrite)) {
    currentSlide <- sheetNamesNoRewrite[slide]
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
    newStoredString <- splitAndReplaceNoRewrite(cell, currentSlide)
    # Repeats when sheetNames ['test.csv', 'sheet1'] instead of ['sheet1', 'test.csv']
    if (!identical(newStoredString, cell)) return(newStoredString)
  }
  original
}

computeStandardRNoRewrite <- function(cell) {
  if (notApplicable(cell)) return(cell)
  # Matrix multiplication expected to be numeric so cell should not be evaluated prior to applyNumeric
  # Should not evaluateCell prior to splitAndReplace, only first element will be used
  storedString <- cell
  if (!grepl('%*%', cell)) {
    nextStoredString <- splitAndReplaceNoRewrite(storedString, NULL)
    while(storedString != nextStoredString) {
      storedString <- nextStoredString
      # matrix(c(1:9), 3), sheet1[1:10,1] for length(cell) > 1
      if (grepl('%*%', storedString) || length(storedString) > 1) break
      nextStoredString <- splitAndReplaceNoRewrite(storedString, NULL)
    }
  }
  numerizeAndSplitNoRewrite(storedString, cell)
}

configureAndComputeNoRewrite <- function(cell) {
  if (notApplicable(cell)) return(cell)
  configureSheetsToMatrixNoRewrite()
  computeStandardRNoRewrite(cell)
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

lattitudeAsDataframeNoRewrite <- function(firstrow, currentLattitude, range) {
  if (firstrow == T) {
    colnames(currentLattitude) <- currentLattitude[1,]
    currentLattitude <- currentLattitude[-1,]
  } else {
    if (length(appendMatchNumbers(range)) != 4) {
      stop('Invalid range.')
    }
    rowLength <- length(evalParse(range)[,1])
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

returnToInitialDataNoRewrite <- function(rows, df) {
  for (row in 1:length(rows)) {
    for (col in 1:length(rows[[1]][[1]])) {
      cell <- rows[[row]][[1]][[col]][[1]]
      if (!is.null(cell) && startsWith(cell)) {
        initialDataNoRewrite$rows[[row]][[1]][[col]][[1]] <- toString(df[row, column][[1]])
      }
    }
  }
}

# #
# ### Get from Cloud Storage
# #
#
# gcs_auth('/tmp/tart-12b0c03bcce1.json')
# gcs_global_bucket('tart-90ca2.appspot.com')
# objects <- gcs_list_objects()
#
# filePath <- args[[1]]
# if (filePath %in% objects$name) {
#   filePosition <- which(filePath == objects$name)
# }
# initialDataNoRewrite <- gcs_get_object(objects$name[[filePosition]])
library(jsonlite)
initialDataNoRewrite <- fromJSON('/Users/eb/Downloads/sparkcase.json', simplifyVector=F)

#
### Load Spark
#

sparkR.session(appName = 'cloudrun-r-sparkr')
sparklyr::spark_install(version = '2.3.0', reset = T)
config <- spark_config()
config$`sparklyr.shell.driver-class-path` <- '/tmp/mysql-connector-java-8.0.20.jar:/tmp/mssql-jdbc-8.4.0.jre8.jar:/tmp/ojdbc8.jar'
sc <- spark_connect(master = 'yarn-client', config = config)

# #
# ### Get Database Credentials
# #
#
# # Create Google KMS service
# gar_auth_service(
# 	'/tmp/tart-90ca2-081a368d40ef.json',
# 	scope = c(
# 		'https://www.googleapis.com/auth/cloud-platform',
# 		'https://www.googleapis.com/auth/cloudkms'))
# decrypt_arg <- list(v1 = 'projects/tart-90ca2/locations/us-central1/keyRings/cloudR-user-database/cryptoKeys/database-login/cryptoKeyVersions/1:asymmetricDecrypt')
# googleKms <- gar_api_generator(
# 	'https://cloudkms.googleapis.com',
# 	'POST',
# 	path_args = decrypt_arg,
# 	data_parse_function = function(x) x$plaintext)
#
# # Get Credentials
# Sys.setenv(GOOGLE_APPLICATION_CREDENTIALS = '/tmp/tart-12b0c03bcce1.json')
# authenticate_datastore('tart-90ca2')
#
# argument1 <- args[[1]]
# authUser <- strsplit(substr(argument1, 6, nchar(argument1)), '\\/')[[1]][1]
# credentials <- lookup('connections', authUser)

#
### Load Tables to Spark
#

rawSheetNamesNoRewrite <- sapply(initialDataNoRewrite, function(x) return(x$name))
sheetNamesNoRewrite <- sapply(rawSheetNamesNoRewrite, function(x) return(gsub(' ', '_', x)), USE.NAMES=F)
typeSheetNoRewrite <- sapply(initialDataNoRewrite, function(x) x$type)
inputNamesNoRewrite <- sheetNamesNoRewrite[typeSheetNoRewrite == 'input']
statOptimNamesNoRewrite <- sheetNamesNoRewrite[typeSheetNoRewrite == 'regression' || typeSheetNoRewrite == 'optimize' ]

for (slide in initialDataNoRewrite) {
  slidename <- gsub(' ', '_', slide$name)
	# if input, load the input to apache Spark with sheet names ie. Sheet1, Sheet2
	if (identical(slide$type, 'input')) {
		databaseCredential <- evalParse(credentials[slide$connection])
		host <- databaseCredential$host$stringValue
		port <- databaseCredential$port$stringValue
		user <- databaseCredential$user$stringValue
		encryptedPassword <- databaseCredential$password$blobValue

		body = list(ciphertext = encryptedPassword)
		password <- googleKms(the_body = body)
		if (identical(slide$delimiter, 'mySQL')) {
			url <- paste0('jdbc:mysql://', host, ':', port, '/', slide$database, '?serverTimezone=UTC')
      options = list(
        url = url,
        user = user,
        password = password,
        dbtable = slide$fileName
      )
      sparkJdbc <- spark_read_jdbc(sc, name=slidename, options)
      eval(call('<-', as.name(slidename), sparkJdbc))
		} else if (identical(slide$delimiter, 'SQLServer')) {
			url <- paste0('jdbc:sqlserver://', host, ':', port)
      options = list(
        url = url,
        user = user,
        password = password,
        dbtable = slide$fileName,
        databaseName = slide$database,
        driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
      )
      sparkJdbc <- spark_read_jdbc(sc, name=slidename, options)
			eval(call('<-', as.name(slidename), sparkJdbc))
		} else if (identical(slide$delimiter, 'OracleDB')) {
			url <- paste0('jdbc:oracle:thin:@', host, ':', port, ':', slide$database)
      options = list(
        url = url,
        user = user,
        password = password,
        dbtable = slide$fileName,
        driver = 'oracle.jdbc.OracleDriver'
      )
      sparkJdbc <- spark_read_jdbc(sc, name=slidename, options)
      eval(call('<-', as.name(slidename), sparkJdbc))
		} else {
			pathToFileSpark <- paste0(args[[2]], slide$file)
      sparkCSV <- spark_read_csv(sc,
        name = slidename,
        path = pathToFileSpark,
        header = T,
        delimiter = slide$delimiter
      )
			eval(call('<-', as.name(slidename), sparkCSV))
		}
	}
}

#
## Compute Spark
#

for (s in 1:length(sheetNamesNoRewrite)) {
  if (initialDataNoRewrite[[s]]$type == 'sheet') {
    name <- sheetNamesNoRewrite[s]
    configureSheetsToMatrixNoRewrite()
    currentSlide <- evalParse(name)
    # currentSlide <- sheet1
    # createLattitudeFromSlide
    # First compute does not require configure. It has been done earlier
    currentLattitude <- apply(currentSlide, 1:2, function(x) computeStandardRNoRewrite(x)[1])
    # Check which sheet is used
    checklist <- sheetNamesNoRewrite
    for (n in sheetNamesNoRewrite) {
      if (any(grepl(n, currentLattitude))) {
        checklist <- checklist[checklist != n]
      }
    }
    checklist <- match(sheetNamesNoRewrite, checklist)
    for (n in 1:length(checklist)) {
      if (is.na(checklist[n])) {
        # Previous sheets might be NA from configureAndComputeNoRewrite
        configureSheetsToMatrixNoRewrite()
        currentLattitude <- evalParse(paste0('`', sheetNamesNoRewrite[n], '`'))
        currentLattitude <- apply(currentLattitude, 1:2, function(x) configureAndComputeNoRewrite(x)[1])
        eval(call('<-', as.name(sheetNamesNoRewrite[n]), currentLattitude))
      }
    }
    # Must splitAndReplaceNoRewrite cells to level 1 cell reference. eval(call()) will replace sheets to numeric causing cell reference to return NA
    currentLattitude <- apply(currentSlide, 1:2, function(x) splitAndReplaceNoRewrite(x, NULL)[1])
    currentLattitude <- apply(currentLattitude, 1:2, function(x) numerizeAndSplitsNoRewrite(x, x)[1])
    eval(call('<-', as.name(name), currentLattitude))
  }
}

#
## Chart
#

for (s in 1:length(sheetNamesNoRewrite)) {
  type <- initialDataNoRewrite[[s]]$type
  if (type == 'chart') {
    range <- gsub(' ', '_', initialDataNoRewrite[[s]]$range)
    types <- evalParse(initialDataNoRewrite[[s]]$types)
    variablex <- initialDataNoRewrite[[s]]$variablex
    variabley <- initialDataNoRewrite[[s]]$variabley
    firstrow <- initialDataNoRewrite[[s]]$firstrow
    name <- sheetNamesNoRewrite[s]
    currentSlide <- sub(RANGE_REGEX, '', range)
    proto_name <- gsub(' ', '_', currentSlide)
    if (proto_name %in% inputNamesNoRewrite) {
      # ggplot spark_apply??
    } else {
      # currentLattitude <- lattitudeAsDataframe(firstrow, evalParse(name), range)
      if (hasArg(variabley)) {
        pngChart <- ggplot(currentLattitude, aes_string(paste0('`',variablex,'`'), paste0('`',variabley,'`'))) + types
      } else {
        pngChart <- ggplot(currentLattitude, aes_string(paste0('`',variablex,'`'))) + types
      }
    }
    ggsave('/tmp/ggplot.png', pngChart)
    initialDataNoRewrite[[s]]$datauri <- base64enc::dataURI(file='/tmp/ggplot.png', mime='image/jpeg')
  }
}

#
## Regression
#

for (s in 1:length(sheetNamesNoRewrite)) {
  type <- initialDataNoRewrite[[s]]$type
  range <- initialDataNoRewrite[[s]]$range
  firstrow <- initialDataNoRewrite[[s]]$firstrow
  regression <- initialDataNoRewrite[[s]]$regression
  rows <- initialDataNoRewrite[[s]]$rows
  currentSlide <- sub(RANGE_REGEX, '', range)
  proto_name <- gsub(' ', '_', currentSlide)
  switch(type,
    'statdesc' = {
      variablesx <- regression$variablesx
      if (proto_name %in% inputNamesNoRewrite) {
        pickup_dropoff <- tryCatch({
          evalParse(proto_name) %>%
            spark_apply(function(e) stat.desc(e[variablesx])) %>%
            collect()
        }, error=function(c) NA)
      } else {
        currentSlide <- sub(currentSlide, name, range)
        currentLattitude <- lattitudeAsDataframeNoRewrite(
          firstrow,
          evalParse(currentSlide),
          currentSlide
        )
        pickup_dropoff <- tryCatch({
          stat.desc(currentLattitude[variablesx])
        }, error=function(c) NA)
      }
    },
    'ftable' = {

    },
  )
  if (pickup_dropoff != NA) {
    pickup_dropoff <- as.matrix(tidy(pickup_dropoff))
    pickup_dropoff <- rbind(colnames(pickup_dropoff), pickup_dropoff)
    returnToInitialDataNoRewrite(rows, currentLattitude)
  }
}

for (s in 1:length(sheetNamesNoRewrite)) {
  type <- initialDataNoRewrite[[s]]$type
  if (initialDataNoRewrite[[s]]$type == 'sheet') {
    # calculate missing references from regression/optimization
    returnToInitialDataNoRewrite(rows, currentLattitude)
  }
}

# #
# # Upload back to cloud storage
# #
#
# json_structure <- function(input, output) {
# 	jsonlite::write_json(
# 		input,
# 		output,
# 		pretty=F,
# 		auto_unbox=T,
# 		null=c('null')
#   )
# }
#
# substrRight <- function(x, n) {
# 	substr(x, nchar(x)-n+1, nchar(x))
# }
# runCounter <- as.numeric(substrRight(max(grep(args[[1]], objects$name, value=T)), 1))
#
# if (is.na(runCounter)) {
# 	runCounter <- 1
# } else {
# 	runCounter <- runCounter + 1
# }
#
# if (grepl('run', args[[1]])) {
# 	newPathToFileGCP <- paste(args[[1]], runCounter, sep=' ')
# } else {
# 	newPathToFileGCP <- paste(args[[1]], 'run', runCounter, sep=' ')
# }
#
# gcs_upload(
# 	initialDataNoRewrite,
# 	name=newPathToFileGCP,
# 	object_function=json_structure)
#
# sparkR.session.stop()

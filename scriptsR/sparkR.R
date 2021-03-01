#
# SparkR.R
#
# Created by Edbert Dudon on 7/8/19.
# Copyright © 2019 Project Tart. All rights reserved.
#
# Resources:
# Mastering Spark with R: https://therinspark.com/
# sparklyr: https://spark.rstudio.com/
# Databases using R: https://db.rstudio.com/
#
# Local testing:
# source('/Users/eb/bac/functionscloud/scriptsR/sparkR.R')
# Sys.setenv(JAVA_HOME = "/Library/Java/JavaVirtualMachines/jdk1.8.0_73.jdk/Contents/Home")
# Sys.setenv(SPARK_HOME = "/usr/local/Cellar/apache-spark/2.4.4")
# initialDataNoRewrite <- jsonlite::fromJSON('/Users/eb/Downloads/sparkcase.json', simplifyVector=F)
# sc <- spark_connect(master = "local", version = "2.4.4")
# stackloss <- copy_to(sc, stackloss, "stackloss")
#
install.packages('googleCloudStorageR', repos = 'http://cran.us.r-project.org')
install.packages('Rcpp', repos='https://rcppcore.github.io/drat')
install.packages('sparklyr', repos = 'http://cran.us.r-project.org')
install.packages('jsonlite', repos = 'http://cran.us.r-project.org')
install.packages('slam', repos = 'http://cran.us.r-project.org')
install.packages('base64enc', repos = 'http://cran.us.r-project.org')
devtools::install_github('cloudyr/rdatastore')

library(SparkR)
library(sparklyr)
library(slam)
# dplyr

options(sparklyr.sanitize.column.names = FALSE)

args <- commandArgs(trailing = T)

#
### Get from Cloud Storage
#

Sys.setenv("GCS_AUTH_FILE" = '/tmp/tart-90ca2-081a368d40ef.json')
Sys.setenv("GCS_DEFAULT_BUCKET" = 'tart-90ca2.appspot.com')
# gcs_auth('/tmp/tart-12b0c03bcce1.json')
# gcs_global_bucket('tart-90ca2.appspot.com')
initialDataNoRewrite <- googleCloudStorageR::gcs_get_object(args[[1]])

#
# ## Load Spark
#

sparkR.session(appName = 'cloudrun-r-sparkr')
sparklyr::spark_install(version = '2.3.0', reset = T)
sc <- spark_connect(master = 'yarn-client')

#
### Load Tables to Spark
#

rawSheetNamesNoRewrite <- sapply(initialDataNoRewrite, function(x) return(x$name))
sheetNamesNoRewrite <- sapply(rawSheetNamesNoRewrite, function(x) return(gsub(' ', '_', x)), USE.NAMES=F)
typeSheetNoRewrite <- sapply(initialDataNoRewrite, function(x) x$type)
inputNamesNoRewrite <- sheetNamesNoRewrite[typeSheetNoRewrite == 'input']

REGRESSION_TYPES <- c(
  'statdesc', 'onewaytable', 'twowaytable', 'threewaytable', 'chisqtest', 'fishertest', 'mantelhaentest',
  'cor', 'cov', 'cortest', 'onettest', 'pairedttest', 'independentttest', 'proptest', 'wilcoxtest',
  'pairedwilcoxtest', 'pairwisewilcoxtext', 'kruskaltest', 'friedmantest', 'coef', 'confint', 'fitted',
  'residuals', 'vcov', 'aic', 'predict', 'simplelinreg', 'linreg', 'binomreg', 'gammareg', 'inversegammareg',
  'poissonreg', 'quasireg', 'quasibinomreg', 'quasipoissonreg', 'durbinwatson', 'ncvtest', 'outliertest',
  'varianceinflation', 'influence', 'companova', 'anova', 'onewayaov', 'tukeyhsd', 'ancovawith1cov',
  'twowayaov', 'twowayancovawith2cov', 'randomaov', 'onewaywithinaov', 'repeatedmeasuresaov', 'twowaywithinaov',
  'twowaybetweenaov', 'manova', 'onewayman', 'robustonewayman', 'randommanova', 'twowaymanova', 'covmanova',
  'onewaywithinmanova', 'twowaywithinmanova', 'twowaybetweenmanova', 'bartletttest', 'flignertest', 'hovtest',
  'binomtest', 'ansaritest', 'moodtest', 'onewaytest', 'quadetest'
)

statOptimNamesNoRewrite <- sheetNamesNoRewrite[typeSheetNoRewrite %in% REGRESSION_TYPES || typeSheetNoRewrite == 'optimize']

evalParse <- function(cell) {
  eval(parse(text=cell))
}

for (slide in initialDataNoRewrite) {
  type <- slide$type
	# if input, load the input to apache Spark with sheet names ie. Sheet1, Sheet2
	if (identical(type, 'input')) {
		if (!require('odbc')) {
			install.packages('odbc', repos = 'http://cran.us.r-project.org')
		}
		if (!require('DBI')) {
			install.packages('DBI', repos = 'http://cran.us.r-project.org')
		}

		#
		### Get Database Credentials
		#

		# Create Google KMS service
		googleAuthR::gar_auth_service(
			'/tmp/tart-90ca2-081a368d40ef.json',
			scope = c(
				# 'https://www.googleapis.com/auth/cloud-platform',
				'https://www.googleapis.com/auth/cloudkms',
				'https://www.googleapis.com/auth/datastore',
		  )
		)
		decrypt_arg <- 'projects/tart-90ca2/locations/us-central1/keyRings/cloudR-user-database/cryptoKeys/database-login/cryptoKeyVersions/1:asymmetricDecrypt'
		googleKms <- googleAuthR::gar_api_generator(
			'https://cloudkms.googleapis.com',
			'POST',
			path_args = list(v1 = decrypt_arg),
			data_parse_function = function(x) rawToChar(base64enc::base64decode(x$plaintext))
		)

    # Create Google Firestore service
		authUser <- strsplit(substr(argument1, 6, nchar(args[[1]])), '\\/')[[1]][1]
		firestore_arg <- paste0('projects/tart-90ca2/databases/(default)/documents/connections/', args[[1]])
		googleFirestore <- googleAuthR::gar_api_generator(
			'https://firestore.googleapis.com',
			'GET',
			path_args = list(v1 = firestore_arg),
			data_parse_function = function(x) x$fields
		)
    connections <- googleFirestore()

    slidename <- gsub(' ', '_', slide$name)
    connector <- slide$input$connector
    connection <- slide$input$connection
    database <- slide$input$database
    dbTable <- slide$input$table
    
		credentials <- connections[connection][[1]][[1]][[1]]
		host <- credentials$host$stringValue
		port <- credentials$port$stringValue
		user <- credentials$user$stringValue

    switch(connector,
      'mySQL' = {
        con <- DBI::dbConnect(odbc::odbc(),
          Driver = "MySQL ODBC 8.0 Driver",
          Server = host,
          UID = user,
          PWD = googleKms(the_body = list(ciphertext = databaseCredential$password$bytesValue)),
          Port = port
        )
  			eval(call('<-', as.name(slidename), tbl(con, dbTable)))
      },
      'SQLServer' = {
        con <- DBI::dbConnect(odbc::odbc(),
          Driver = "ODBC Driver 17 for SQL Server",
          Server = host,
          Database = database,
          UID = user,
          PWD = googleKms(the_body = list(ciphertext = databaseCredential$password$bytesValue)),
          Port = port
        )
  			eval(call('<-', as.name(slidename), tbl(con, dbTable)))
      },
      'OracleDB' = {
        con <- DBI::dbConnect(odbc::odbc(),
          Driver = "Oracle 19c ODBC driver",
          Host = host,
          SVC = database,
          UID = user,
          PWD = googleKms(the_body = list(ciphertext = databaseCredential$password$bytesValue)),
          Port = port
        )
        eval(call('<-', as.name(slidename), tbl(con, dbTable)))
      },
      'PostgreSQL' = {
        con <- DBI::dbConnect(odbc::odbc(),
          Driver = "PostgreSQL Driver",
          Server = host,
          Database = database,
          UID = user,
          PWD = googleKms(the_body = list(ciphertext = databaseCredential$password$bytesValue)),
          Port = port
        )
        eval(call('<-', as.name(slidename), tbl(con, dbTable)))
      },
      # {
      #   pathToFileSpark <- paste0(args[[2]], slide$file)
      #   sparkCSV <- spark_read_csv(sc,
      #     name = slidename,
      #     path = pathToFileSpark,
      #     header = T,
      #     delimiter = slide$delimiter
      #   )
  		# 	eval(call('<-', as.name(slidename), sparkCSV))
      # }
    )
	}
}

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

FUNCTIONS_WITH_PREFIX <- c(
  'rnorm(', 'rexp(', 'rgamma(', 'rpois(', 'rweibull(', 'rcauchy(', 'rbeta(', 'rt(', 'rf(',
	'rbinom(', 'rgeom(', 'rhyper(', 'rlogis(', 'rlnorm(', 'rnbinom(', 'runif(', 'rwilcox('
)

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

getMaxRowsFromInitial <- function(rows) {
  as.numeric(names(tail(rows, n=1)))+1
}

getMaxColumnsFromInitial <- function(rows) {
  columns <- 1
  for (row in rows) {
    n <- max(as.numeric(names(row$cells)))
    if (n > columns) {
      columns <- n
    }
  }
  columns + 1
}

configureSheetsToMatrixNoRewrite <- function() {
  for (s in 1:length(sheetNamesNoRewrite)) {
    currentLattitude <- initialDataNoRewrite[[s]]$rows
    currentLattitude <- currentLattitude[names(currentLattitude) != 'len']
    rows <- getMaxRowsFromInitial(currentLattitude)
    columns <- getMaxColumnsFromInitial(currentLattitude)
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
            sdf_collect()
        }, error=function(c) NA)

        ### Matrix plus sparklyr????
        if (length(pickup_dropoff) == 1) {
          nextResult <- pickup_dropoff
        }
      # Prevent Statistics/Optimizations cell references from being calculated
      } else if (!isFinishedRegOpt && length(proto_name) > 0 && any(grepl(proto_name, statOptimNamesNoRewrite))) {
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
    }
  # )
  # Use silent for cleaner code
  ,silent=T)
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
    }, USE.NAMES=F))
  }

  evaluateParts <- sapply(stringParts, evaluateCellNoRewrite, USE.NAMES=F)

  # Scenario 12 or exp(test.csv[2:10,1]) should return error. Only matrix should output more than one cell.
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

returnSheet <- function(s, df) {
  currentLattitude <- initialDataNoRewrite[[s]]$rows
  currentLattitude <- currentLattitude[names(currentLattitude) != 'len']
  rows <- getMaxRowsFromInitial(currentLattitude)
  columns <- getMaxColumnsFromInitial(currentLattitude)

  for (r in 1:rows) {
    for (c in 1:columns) {
      value <- currentLattitude[[toString(r-1)]]$cells[[toString(c-1)]]$text
      if (!is.null(value) && startsWith(value, '=')) {
        initialDataNoRewrite[[s]]$rows[[toString(r-1)]]$cells[[toString(c-1)]]$text <<- toString(df[r, c][[1]])
      }
    }
  }
}

createLattitudeFromSlide <- function(s) {
  name <- sheetNamesNoRewrite[s]
  configureSheetsToMatrixNoRewrite()
  currentSlide <- evalParse(name)
  currentLattitude <- apply(currentSlide, 1:2, function(x) configureAndComputeNoRewrite(x)[1])

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
      eval(call('<<-', as.name(sheetNamesNoRewrite[n]), currentLattitude))
    }
  }

  # Must splitAndReplaceNoRewrite cells to level 1 cell reference. eval(call()) will replace sheets to numeric causing cell reference to return NA
  currentLattitude <- apply(currentSlide, 1:2, function(x) splitAndReplaceNoRewrite(x, NULL)[1])
  currentLattitude <- apply(currentLattitude, 1:2, function(x) numerizeAndSplitNoRewrite(x, x)[1])

  # return to initialDataNoRewrite, else results will be rewritten in next iteration
  returnSheet(s, currentLattitude)
}

#
## Compute Spark
#

isFinishedRegOpt <- FALSE
for (s in 1:length(sheetNamesNoRewrite)) {
  type <- initialDataNoRewrite[[s]]$type
  if (type == 'sheet') {
    createLattitudeFromSlide(s)
  }
}

#
## Compute Matrix
#

#
## Function Required for Regression/Optimization/Chart
#

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
    } else {
      # sheet1[2,]
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
    } else {
      # sheet1[,2:2]
      rowLength <- nrow(evalParse(matchSlide))
      result <- c(1, rowLength, matchNumbers)
    }
  } else if (length(matchNumbers) == 3 && grepl('\\d+\\:{1}\\d+\\,{1}\\d+|\\d+\\,{1}\\d+\\:{1}\\d+', matchRange)) {
    # sheet[1:2,2]
    if (grepl('\\d+\\:{1}\\d+\\,{1}\\d+', matchRange)) {
      result <- append(matchNumbers, matchNumbers[3], after=3)
    } else {
      # sheet[1,1:2]
      result <- append(matchNumbers, matchNumbers[1], after=0)
    }
  } else {
    result <- matchNumbers
  }
  return(as.numeric(result))
}

lattitudeAsDataframeNoRewrite <- function(firstrow, currentLattitude, range) {
  if (firstrow) {
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
    if (length(appendMatchNumbers(range)) != 4) stop('Invalid range.')

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

#
## Regression
#

colnamesToRow <- function(lattitude) {
  rbind(colnames(lattitude), lattitude)
}

returnStatistic <- function(df, s) {
  lattitude <- df %>%
    sdf_collect() %>%
    as.matrix() %>%
    colnamesToRow()

  for (r in 1:nrow(lattitude)) {
    for (c in 1:ncol(lattitude)) {
      initialDataNoRewrite[[s]]$rows[[toString(r-1)]]$cells[[toString(c-1)]]$text <<- lattitude[r,c][[1]]
    }
  }
  initialDataNoRewrite[[s]]$regression$sample <<- FALSE
}

returnStatisticRowCol2 <- function(df, s) {
  lattitude <- df %>%
    sdf_collect() %>%
    tibble::rownames_to_column() %>%
    as.matrix() %>%
    colnamesToRow()

  for (r in 2:nrow(lattitude)) {
    for (c in 2:ncol(lattitude)) {
      initialDataNoRewrite[[s]]$rows[[toString(r-1)]]$cells[[toString(c-1)]]$text <<- lattitude[r,c][[1]]
    }
  }
  initialDataNoRewrite[[s]]$regression$sample <<- FALSE
}

summaryStatistics <- function(x, na.omit=FALSE) {
  if (na.omit)
    x <- x[!is.na(x)]
  m <- mean(x)
  n <- length(x)
  s <- sd(x)
  l <- min(x)
  h <- max(x)

  return(c(count=n, mean=m, stdev=s, min=l, max=h))
}

gsubFullstop <- function(x) {
  gsub('\\.', '_', x)
}

sparkApplyColnames <- function(proto_name, customfunction, context) {
  try(
    evalParse(proto_name) %>%
      spark_apply(customfunction, context=context) %>%
      returnStatistic(s)
  )
}

setMethodCorrelation <- function(method) {
  if (is.null(method)) {
    return('pearson')
  }

  if (method == 'p') {
    return('pearson')
  }

  if (method == 's') {
    return('spearman')
  }
}

setConfidenceLevel <- function(confidencelevel) {
  if (is.null(confidencelevel)) {
    return(0.95)
  }
  confidencelevel
}

setPenalty <- function(penalty) {
  if (is.null(penalty)) {
    return(2)
  }
  penalty
}

setBootstrapMethod <- function(method) {
  if (is.null(method)) {
    return('r')
  }
  method
}

setAlternative <- function(alternative) {
  if (is.null(alternative)) {
    return('t')
  }
  alternative
}

# Gets post-calcuated sheets into R with its respective names
configureSheetsToMatrixNoRewrite()
for (s in 1:length(sheetNamesNoRewrite)) {
  regression <- initialDataNoRewrite[[s]]$regression
  if (!is.null(regression)) {
		if (!require('broom')) {
			install.packages('broom', repos = 'http://cran.us.r-project.org')
		}

    range <- regression$range
    currentSlide <- sub(RANGE_REGEX, '', range)
    proto_name <- gsub(' ', '_', currentSlide)
    isInput <- proto_name %in% inputNamesNoRewrite
    firstrow <- regression$firstrow

    if (!isInput || !(firstrow == 'true')) next

    type <- initialDataNoRewrite[[s]]$type

    switch(type,
      # Descriptive Statistics
      'statdesc' = {
        # stackloss %>% sdf_describe() %>% returnStatistic(s)
        variablesx <- jsonlite::fromJSON(regression$variablesx)
        try(
          evalParse(proto_name) %>%
            sdf_describe(cols=variablesx) %>%
            returnStatistic(s)
        )
      },
      # Frequency Table
      'onewaytable' = {
        variablex <- gsubFullstop(regression$variablex)
        try(
          evalParse(proto_name) %>%
            count(variablex) %>%
            select(-.add) %>%
            returnStatistic(s)
        )
      },
      # Cross Table
      'twowaytable' = {
        variablex <- gsubFullstop(regression$variablex)
        variabley <- gsubFullstop(regression$variabley)
        try(
          evalParse(proto_name) %>%
            sdf_crosstab(variablex, variabley) %>%
            returnStatistic(s)
        )
      },
      # Tests of Independence
      'chisq.test' = {
        variablex <- gsubFullstop(regression$variablex)
        variabley <- gsubFullstop(regression$variabley)
        try(
          evalParse(proto_name) %>%
            ml_chisquare_test(variablex, variabley) %>%
            returnStatistic(s)
        )
      },
      # Correlations
      'cor' = {
				if (!require('corrr')) {
					install.packages('corrr', repos = 'http://cran.us.r-project.org')
				}
        variablesx <- jsonlite::fromJSON(regression$variablesx)
        method <- setMethodCorrelation(regression$method)
        try(
          evalParse(proto_name) %>%
            select(variablesx) %>%
            corrr::correlate(method=method) %>%
            returnStatistic(s)
        )
      },
      # Fitting Linear Models Tools
      'coef' = {
        formula <- gsubFullstop(regression$formula)
        try(
          evalParse(proto_name) %>%
            ml_linear_regression(evalParse(formula)) %>%
            coef() %>%
            broom::tidy() %>%
            rename(term=names, coefficient=x) %>%
            returnStatistic(s)
        )
      },
      'confint' = {
        context <- list(
          formula = gsubFullstop(regression$formula),
          confidencelevel = evalParse(
            setConfidenceLevel(regression$confidencelevel)
          )
        )
        try(
          evalParse(proto_name) %>%
            spark_apply(function(e, context) {
              confint(
                lm(eval(parse(text=context$formula)), e),
                level=context$confidencelevel
              )
            }, context=context) %>%
            returnStatisticRowCol2()
        )
      },
      'fitted' = {
        formula <- gsubFullstop(regression$formula)
        try(
          evalParse(proto_name) %>%
            ml_linear_regression(evalParse(formula)) %>%
            fitted() %>%
            as.matrix() %>%
            returnStatistic(s)
        )
      },
      'residuals' = {
        formula <- gsubFullstop(regression$formula)
        try(
          evalParse(proto_name) %>%
            spark_apply(function(e, formula) {
              broom::tidy(residuals(
                lm(eval(parse(text=formula)), e)
              ))
            }, context=formula) %>%
            rename(id=names, residuals=x) %>%
            returnStatistic(s)
        )
      },
      'vcov' = {
        formula <- gsubFullstop(regression$formula)
        try(
          evalParse(proto_name) %>%
            spark_apply(function(e, formula) {
              vcov(lm(eval(parse(text=formula)), e))
            }, context=formula) %>%
            returnStatisticRowCol2()
        )
      },
      'aic' = {
        context <- list(
          formula = gsubFullstop(regression$formula),
          penalty = evalParse(
            setPenalty(regression$penalty)
          )
        )
        try(
          evalParse(proto_name) %>%
            spark_apply(function(e, formula) {
              AIC(
                lm(eval(parse(text=context$formula)), e),
                k=context$penalty
              )
            }, context=formula) %>%
            rename(aic=result) %>%
            returnStatistic(s)
        )
      },
      'predict' = {
        formula <- gsubFullstop(regression$formula)
        try(
          evalParse(proto_name) %>%
            ml_linear_regression(evalParse(formula)) %>%
            predict() %>%
            as.matrix() %>%
            returnStatistic(s)
        )
      },
      'simplelinreg' = {
        variablex <- gsubFullstop(regression$variablex)
        variabley <- gsubFullstop(regression$variabley)
        formula <- paste0(variabley, '~', variablex)
        try(
          evalParse(proto_name) %>%
            ml_linear_regression(evalParse(formula)) %>%
            broom::tidy() %>%
            returnStatistic(s)
        )
      },
      'linreg' = {
        formula <- gsubFullstop(regression$formula)
        try(
          evalParse(proto_name) %>%
            ml_linear_regression(evalParse(formula)) %>%
            broom::tidy() %>%
            returnStatistic(s)
        )
      },
      'durbinwatson' = {
        context <- list(
          formula = gsubFullstop(regression$formula),
          lag = evalParse(
            setLag(regression$lag)
          ),
          method = setBootstrapMethod(regression$method),
          alternative = setAlternative(regression$alternative)
        )
        # sparkApplyColnames(proto_name, function(e, context) {
        #   broom::tidy(dwt(
        #     lm(eval(parse(text=context$formula)), e),
        #     max.lag=context$lag,
        #     method=context$method,
        #     alternative=context$alternative
        #   ))
        # }, context)
      },
      'ncvtest' = {
				if (!require('car')) {
				  install.packages('car', repos = 'http://cran.us.r-project.org')
				}
        formula <- gsubFullstop(regression$formula)
        # sparkApplyColnames(proto_name, function(e, formula) {
        #   car::ncvTest(lm(eval(parse(text=formula)), e))
        # }, formula)
      },
      'outliertest' = {
        formula <- gsubFullstop(regression$formula)
        # sparkApplyColnames(proto_name, function(e, formula) {
        #   broom::tidy(lm(eval(parse(text=formula)), e))
        # }, formula)
      },
      'varianceinflation' = {
				if (!require('car')) {
				  install.packages('car', repos = 'http://cran.us.r-project.org')
				}
        formula <- gsubFullstop(regression$formula)
        # sparkApplyColnames(proto_name, function(e, formula) {
        #   broom::tidy(car::vif(lm(eval(parse(text=formula)), e)))
        # }, formula)
      },
      'aov' = {
        formula <- gsubFullstop(regression$formula)
        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(aov(eval(parse(text=formula)), e))
        }, formula)
      },
      'onewayaov' = {
        variablex <- gsubFullstop(regression$variablex)
        variabley <- gsubFullstop(regression$variabley)
        formula <- paste0(variabley, '~', variablex)

        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(aov(eval(parse(text=formula)), e))
        }, formula)
      },
      'tukeyhsd' = {
        context <- list(
          formula = gsubFullstop(regression$formula),
          confidencelevel = evalParse(
            setConfidenceLevel(regression$confidencelevel)
          )
        )
        # sparkApplyColnames(proto_name, function(e, context) {
        #   broom::tidy(TukeyHSD(
        #     aov(eval(parse(text=context$formula)), e),
        #     conf.level=context$confidencelevel
        #   ))
        # }, context)
      },
      'ancovawith1cov' = {
        variablex <- gsubFullstop(regression$variablex)
        variabley <- gsubFullstop(regression$variabley)
        covariate1 <- gsubFullstop(regression$covariate1)
        formula <- paste0(variabley, '~', covariate1, '+', variablex)

        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(aov(eval(parse(text=formula)), e))
        }, formula)
      },
      'twowayaov' = {
        variablex1 <- gsubFullstop(regression$variablex1)
        variablex2 <- gsubFullstop(regression$variablex2)
        variabley <- gsubFullstop(regression$variabley)
        formula <- paste0(variabley, '~', variablex1, '*', variablex2)

        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(aov(eval(parse(text=formula)), e))
        }, formula)
      },
      'twowayancovawith2cov' = {
        variablex1 <- gsubFullstop(regression$variablex1)
        variablex2 <- gsubFullstop(regression$variablex2)
        variabley <- gsubFullstop(regression$variabley)
        covariate1 <- gsubFullstop(regression$covariate1)
        covariate2 <- gsubFullstop(regression$covariate2)
        formula <- paste0(variabley, '~', covariate1, '+', covariate2, '+', variablex1, '*', variablex2)

        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(aov(eval(parse(text=formula)), e))
        }, formula)
      },
      'randomaov' = {
        variablex <- gsubFullstop(regression$variablex)
        variabley <- gsubFullstop(regression$variabley)
        blocks <- gsubFullstop(regression$blocks)
        formula <- paste0(variabley, '~', blocks, '+', variablex)

        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(aov(eval(parse(text=formula)), e))
        }, formula)
      },
      'onewaywithinaov' = {
        variablex <- gsubFullstop(regression$variablex)
        variabley <- gsubFullstop(regression$variabley)
        subjects <- gsubFullstop(regression$subjects)
        formula <- paste0(variabley, '~', variablex, '+Error(', subjects, '/', variablex1, ')')

        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(aov(eval(parse(text=formula)), e))
        }, formula)
      },
      'repeatedmeasuresaov' = {
        variablex1 <- gsubFullstop(regression$variablex1)
        variablex2 <- gsubFullstop(regression$variablex2)
        variabley <- gsubFullstop(regression$variabley)
        subjects <- gsubFullstop(regression$subjects)
        formula <- paste0(variabley, '~', variablex2, '*', variablex1, '+Error(', subjects, '/', variablex1, ')')

        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(aov(eval(parse(text=formula)), e))
        }, formula)
      },
      'manova' = {
        formula <- gsubFullstop(regression$formula)
        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(manova(eval(parse(text=formula)), e))
        }, formula)
      },
      'onewayman' = {
        variablex <- gsubFullstop(regression$variablex)
        variablesy <- gsubFullstop(
          jsonlite::fromJSON(regression$variablesy)
        )
        formula <- paste0('cbind(', paste0(variablesy, collapse=','), ')~`', variablex, '`')

        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(manova(eval(parse(text=formula)), e))
        }, formula)
      },
      # 'robustonewayman' = {
				# if (!require('rrcov')) {
				#   install.packages('rrcov', repos = 'http://cran.us.r-project.org')
				# }
        # context <- list(
        #   variablex = gsubFullstop(regression$variablex),
        #   variablesy = gsubFullstop(
        #     jsonlite::fromJSON(regression$variablesy)
        #   ),
        #   method=regression$method,
        #   approximation=regression$approximation,
        # )
        #
        # sparkApplyColnames(proto_name, function(e, context) {
        #   broom::tidy(
        #     rrcov::Wilks.test(
        #       context$variablesy,
        #       context$variablex,
        #       method=context$method,
        #       approximation=context$approximation
        #     )
        #   )
        # }, context)
      # },
      # 'bartletttest' = {
      #   variablex <- gsubFullstop(regression$variablex)
      #   variabley <- gsubFullstop(regression$variabley)
      #   formula <- paste0(variabley, '~', variablex)
      #
      #   sparkApplyColnames(proto_name, function(e, formula) {
      #     broom::tidy(bartlett.test(eval(parse(text=formula)), e))
      #   }, formula)
      # },
      'flignertest' = {
        variablex <- gsubFullstop(regression$variablex)
        variabley <- gsubFullstop(regression$variabley)
        formula <- paste0(variabley, '~', variablex)

        sparkApplyColnames(proto_name, function(e, formula) {
          broom::tidy(fligner.test(eval(parse(text=formula)), e))
        }, formula)
      },
    )
  }

  #
  ## Optimization
  #

  optimization <- initialDataNoRewrite[[s]]$optimization

  if (!is.null(optimization)) {
		if (!require('ROI')) {
			install.packages('ROI', repos = 'http://cran.us.r-project.org')
		}
		if (!require('ROI.plugin.glpk')) {
		install.packages('ROI.plugin.glpk', repos = 'http://cran.us.r-project.org')
		}
		if (!require('ROI.plugin.qpoases')) {
		install.packages('ROI.plugin.qpoases', repos = 'http://cran.us.r-project.org')
		}
		if (!require('ROI.plugin.optimx')) {
		install.packages('ROI.plugin.optimx', repos = 'http://cran.us.r-project.org')
		}
		if (!require('ROI.plugin.lpsolve')) {
		install.packages('ROI.plugin.lpsolve', repos = 'http://cran.us.r-project.org')
		}
		if (!require('ROI.plugin.quadprog')) {
		install.packages('ROI.plugin.quadprog', repos = 'http://cran.us.r-project.org')
		}
		if (!require('ROI.plugin.nloptr')) {
		install.packages('ROI.plugin.nloptr', repos = 'http://cran.us.r-project.org')
		}
		if (!require('ROI.plugin.ecos')) {
			install.packages('ROI.plugin.ecos', repos = 'http://cran.us.r-project.org')
		}

		library(ROI)
		library(ROI.plugin.glpk)
		library(ROI.plugin.qpoases)
		library(ROI.plugin.optimx)
		library(ROI.plugin.lpsolve)
		library(ROI.plugin.quadprog)
		library(ROI.plugin.nloptr)
		library(ROI.plugin.ecos)
  }
}

CHARTS_TYPES = c(
  'geom_bar()', 'geom_area(stat="bin")', 'geom_density(kernel="gaussian")', 'geom_dotplot()', 'geom_freqpoly()',
  'geom_histogram(binwidth=5)', 'geom_jitter(height=2, weight=2)', 'geom_point()', 'geom_quantile()', 'geom_rug(sides="bl")',
  'geom_smooth()', 'geom_boxplot()', 'geom_dotplot()', 'geom_violin(scale="area")', 'geom_count()',
  'geom_bin2d(binwidth=c(0.25,500))', 'geom_density2d()', 'geom_hex()', 'geom_area()', 'geom_line()', 'geom_step(direction="hv")'
)

#
## Chart
#

for (s in 1:length(sheetNamesNoRewrite)) {
  charts <- initialDataNoRewrite[[s]]$charts
  for (chart in charts) {
		if (!require('dbplot')) {
			install.packages('dbplot', repos = 'http://cran.us.r-project.org')
		}
		if (!require('ggplot2')) {
			install.packages('ggplot2', repos = 'http://cran.us.r-project.org')
		}

    range <- gsub(' ', '_', translateRForProcess(chart$range, s))
    currentSlide <- sub(RANGE_REGEX, '', range)
    proto_name <- gsub(' ', '_', currentSlide)
    isInput <- proto_name %in% inputNamesNoRewrite
    firstrow <- chart$firstrow

    if (!isInput || !(firstrow == 'true')) next

    types <- sapply(chart$types, function(x) CHARTS_TYPES[x+1])
    variables <- evalParse(proto_name) %>% tbl_vars()
    variablex <- variables[chart$variablex+1]
    variabley <- variables[chart$variabley+1]

    if (proto_name %in% inputNamesNoRewrite && length(types) == 1) {
      switch(types,
        'geom_bar()' = {
          pngChart <- evalParse(proto_name) %>%
            dbplot_bar(variablex)
        },
        'geom_area(stat="bin")' = {},
        'geom_density(kernel="gaussian")' = {},
        'geom_dotplot()' = {},
        # 'geom_dotplot()' = {}, 2 vars
        'geom_freqpoly()' = {},
        'geom_histogram(binwidth=5)' = {
          pngChart <- evalParse(proto_name) %>%
            dbplot_histogram(variablex, binwidth = 5)
        },
        'geom_jitter(height=2, weight=2)' = {},
        'geom_point()' = {},
        'geom_quantile()' = {},
        'geom_rug(sides="bl")' = {},
        'geom_boxplot()' = {
          pngChart <- evalParse(proto_name) %>%
            dbplot_boxplot(variablex, variabley)
        },
        'geom_violin(scale="area")' = {},
        'geom_count()' = {},
        'geom_bin2d(binwidth=c(0.25,500))' = {},
        'geom_density2d()' = {},
        'geom_hex()' = {},
        'geom_area()' = {},
        'geom_line()' = {
          pngChart <- evalParse(proto_name) %>%
            dbplot_line(variablex, variabley)
        },
        'geom_step(direction="hv")' = {},
        'raster' = {
          pngChart <- evalParse(proto_name) %>%
            dbplot_raster(variablex, variabley)
        },
      )
    }

    ggplot2::ggsave('/tmp/ggplot.png', pngChart)
    initialDataNoRewrite[[s]]$sparkuri <- base64enc::dataURI(file='/tmp/ggplot.png', mime='image/jpeg')
  }
}

#
## Finish calculating Missing Values
#

isFinishedRegOpt <- TRUE
for (s in 1:length(sheetNamesNoRewrite)) {
  type <- initialDataNoRewrite[[s]]$type
  if (initialDataNoRewrite[[s]]$type == 'sheet') {
    # calculate missing references from regression/optimization
    createLattitudeFromSlide(s)
  }
}

#
# Upload back to cloud storage
#

json_structure <- function(input, output) {
	jsonlite::write_json(
		input,
		output,
		pretty=F,
		auto_unbox=T,
		null=c('null')
  )
}

substrRight <- function(x, n) {
	substr(x, nchar(x)-n+1, nchar(x))
}
runCounter <- as.numeric(substrRight(max(grep(args[[1]], objects$name, value=T)), 1))

if (is.na(runCounter)) {
	runCounter <- 1
} else {
	runCounter <- runCounter + 1
}

if (grepl('run', args[[1]])) {
	newPathToFileGCP <- paste(args[[1]], runCounter, sep=' ')
} else {
	newPathToFileGCP <- paste(args[[1]], 'run', runCounter, sep=' ')
}

googleCloudStorageR::gcs_upload(
	initialDataNoRewrite,
	name=newPathToFileGCP,
	object_function=json_structure)

sparkR.session.stop()

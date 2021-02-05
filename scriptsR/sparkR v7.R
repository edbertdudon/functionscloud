#
# SparkR.R
#
# Created by Edbert Dudon on 7/8/19.
# Copyright © 2019 Project Tart. All rights reserved.
#
install.packages("Rcpp", repos="https://rcppcore.github.io/drat")
install.packages('sparklyr', repos = "http://cran.us.r-project.org")
install.packages("googleCloudStorageR", repos = "http://cran.us.r-project.org")
install.packages("jsonlite", repos = "http://cran.us.r-project.org")
install.packages("ggplot2", repos = "http://cran.us.r-project.org")
install.packages("base64enc", repos = "http://cran.us.r-project.org")
install.packages("broom", repos = "http://cran.us.r-project.org")
devtools::install_github("cloudyr/rdatastore")

library(googleCloudStorageR)
library(ggplot2)
library(SparkR)
library(sparklyr)
library(googleAuthR)
library(rdatastore)
library(broom)

args <- commandArgs(trailing = T)

#
### Get from Cloud Storage
#
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

sparkR.session(appName = "cloudrun-r-sparkr")
sparklyr::spark_install(version = "2.3.0", reset = T)
config <- spark_config()
config$`sparklyr.shell.driver-class-path` <- "/tmp/mysql-connector-java-8.0.20.jar:/tmp/mssql-jdbc-8.4.0.jre8.jar:/tmp/ojdbc8.jar"
sc <- spark_connect(master = "yarn-client", config = config)

#
### Get Database Credentials
#

# Create Google KMS service
gar_auth_service(
	"/tmp/tart-90ca2-081a368d40ef.json",
	scope = c(
		"https://www.googleapis.com/auth/cloud-platform",
		"https://www.googleapis.com/auth/cloudkms"))
decrypt_arg <- list(v1 = "projects/tart-90ca2/locations/us-central1/keyRings/cloudR-user-database/cryptoKeys/database-login/cryptoKeyVersions/1:asymmetricDecrypt")
googleKms <- gar_api_generator(
	"https://cloudkms.googleapis.com",
	"POST",
	path_args = decrypt_arg,
	data_parse_function = function(x) x$plaintext)

# Get Credentials
Sys.setenv(GOOGLE_APPLICATION_CREDENTIALS = "/tmp/tart-12b0c03bcce1.json")
authenticate_datastore("tart-90ca2")

argument1 <- args[[1]]
authUser <- strsplit(substr(argument1, 6, nchar(argument1)), "\\/")[[1]][1]
credentials <- lookup("connections", authUser)

#
### Load Tables to Spark
#

rawSheetNamesNoRewrite <- character(length(initialDataNoRewrite))
sheetNamesNoRewrite <- character(length(initialDataNoRewrite))
for (i in 1:length(initialDataNoRewrite)) {
	rawSheetNamesNoRewrite[i] <- initialDataNoRewrite[[i]]$name
  sheetNamesNoRewrite[i] <- gsub(" ", "_", initialDataNoRewrite[[i]]$name)
}
inputNamesNoRewrite <- sheetNamesNoRewrite
for (slide in initialDataNoRewrite) {
  slidename <- gsub(" ", "_", slide$name)
	# if input, load the input to apache Spark with sheet names ie. Sheet1, Sheet2
	if (identical(slide$type, "input")) {
		databaseCredential <- eval(parse(text=credentials[slide$connection]))
		host <- databaseCredential$host$stringValue
		port <- databaseCredential$port$stringValue
		user <- databaseCredential$user$stringValue
		encryptedPassword <- databaseCredential$password$blobValue

		body = list(ciphertext = encryptedPassword)
		password <- googleKms(the_body = body)
		if (identical(slide$delimiter, "mySQL")) {
			url <- paste0("jdbc:mysql://", host, ":", port, "/", slide$database, "?serverTimezone=UTC")
      options = list(
        url = url,
        user = user,
        password = password,
        dbtable = slide$fileName
      )
      sparkJdbc <- spark_read_jdbc(sc, name=slidename, options)
      eval(call("<-", as.name(slidename), sparkJdbc))
		} else if (identical(slide$delimiter, "SQLServer")) {
			url <- paste0("jdbc:sqlserver://", host, ":", port)
      options = list(
        url = url,
        user = user,
        password = password,
        dbtable = slide$fileName,
        databaseName = slide$database,
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      )
      sparkJdbc <- spark_read_jdbc(sc, name=slidename, options)
			eval(call("<-", as.name(slidename), sparkJdbc))
		} else if (identical(slide$delimiter, "OracleDB")) {
			url <- paste0("jdbc:oracle:thin:@", host, ":", port, ":", slide$database)
      options = list(
        url = url,
        user = user,
        password = password,
        dbtable = slide$fileName,
        driver = "oracle.jdbc.OracleDriver"
      )
      sparkJdbc <- spark_read_jdbc(sc, name=slidename, options)
      eval(call("<-", as.name(slidename), sparkJdbc))
			))
		} else {
			pathToFileSpark <- paste0(args[[2]], slide$file)
      sparkCSV <- spark_read_csv(sc,
        name = slidename,
        path = pathToFileSpark,
        header = T,
        delimiter = slide$delimiter
      )
			eval(call("<-", as.name(slidename), sparkCSV))
		}
	} else {
    inputNamesNoRewrite <- inputNamesNoRewrite[inputNamesNoRewrite != slidename]
  }
}

#
## Function Required for Computation
#

LETTERS_REFERENCE <- letters[1:26]

lettersToColumn <- function(letter) {
	column <- 0
	letterlength <- nchar(letter)
	for (i in 1:letterlength) {
		column <- match(tolower(letter), LETTERS_REFERENCE) * 26^(letterlength-i) + column
	}
	return(column)
}

FUNCTIONS_WITH_PREFIX <- c("rnorm(", "rexp(", "rgamma(", "rpois(", "rweibull(", "rcauchy(", "rbeta(", "rt(", "rf(",
	"rbinom(", "rgeom(", "rhyper(", "rlogis(", "rlnorm(", "rnbinom(", "runif(", "rwilcox(")

addPrefixToFunction <- function(cell) {
	for (func in FUNCTIONS_WITH_PREFIX) {
		cell <- gsub(func, paste0(func, "1,"), cell)
	}
	return(cell)
}

notApplicable <- function(cell) {
  is.null(cell) || is.na(cell) || cell=="" || !is.character(cell)
}

translateRForProccess <- function(cell, currentSlide) {
	if (notApplicable(cell) || !identical(substring(cell,1,1),"=")) return(cell)
	match <- regmatches(cell, gregexpr("[[:upper:]]+\\d*", cell))[[1]]
	coordinates <- gsub("'", "`", substring(cell, 2), fixed=T)
	for (i in 1:length(initialDataNoRewrite)) {
		coordinates <- gsub(rawSheetNamesNoRewrite[i], sheetNamesNoRewrite[i], coordinates)
	}
	if (length(match) == 0) return(coordinates)
	for (i in 1:length(match)) {
    column <- lettersToColumn(regmatches(match[i], regexpr("[[:upper:]]{,2}", match[i])))
    row <- as.numeric(regmatches(match[i], regexpr("\\d+", match[i])))
		if (!identical(i+1, length(match)-1)) {
			column2 <- lettersToColumn(regmatches(match[i+1], regexpr("[[:upper:]]{,2}", match[i+1])))
			row2 <- as.numeric(regmatches(match[i+1], regexpr("\\d+", match[i+1])))
			if (!grepl("\\d", match[i])) {
				ref5 <- paste0("[,", column, ":", column2, "]")
				prefix5 <- paste0("!", match[i], ":", match[i+1])
				coordinates <- gsub(prefix5, ref5, coordinates)
				ref6 <- paste0("`", sheetNamesNoRewrite[currentSlide], "`[,", column, ":", column2, "]")
				prefix6 <- paste0(match[i], ":", match[i+1])
				coordinates <- gsub(prefix6, ref6, coordinates)
			}
			ref4 <- paste0("[", row, ":", row2, ",", column, ":", column2, "]")
			prefix4 <- paste0("!", match[i], ":", match[i+1])
			coordinates <- gsub(prefix4, ref4,coordinates)
			ref3 <- paste0("`", sheetNamesNoRewrite[currentSlide], "`[", row, ":", row2, ",", column, ":", column2, "]")
			prefix3 <- paste0(match[i], ":", match[i+1])
			coordinates <- gsub(prefix3, ref3, coordinates)
		}
		if (grepl("\\d", match[i])) {
			ref <- paste0("[", row, ",", column, "]")
			prefix <- paste0("!", match[i])
			coordinates <- gsub(prefix, ref, coordinates)
			ref2 <- paste0("`", sheetNamesNoRewrite[currentSlide], "`[", row, ",", column, "]")
			prefix2 <- match[i]
			coordinates <- gsub(prefix2, ref2, coordinates)
		}
	}
	return(addPrefixToFunction(coordinates))
}

applyToSheets <- function(customfunction) {
  for (s in 1:length(initialDataNoRewrite)) {
    type <- initialDataNoRewrite[[s]]$type
    if (type == "sheet") {
      name <- sheetNamesNoRewrite[s]
      customfunction(s, name)
    }
  }
}

configureSheetsToMatrixNoRewrite <- function() {
  applyToSheets(function(s, name) {
    name <- sheetNamesNoRewrite[s]
    rows <- initialDataNoRewrite[[s]]$rows
    currentLattitude <- rows[names(rows) != "len"]
    # rows <- rows[names(rows) != "height"]
    currentLattitude <- matrix(unlist(currentLattitude), nrow=length(currentLattitude), byrow=T)
    currentLattitude <- apply(currentLattitude, 1:2, function(x) translateRForProccess(x, s))
    eval(call("<-", as.name(name), currentLattitude))
  })
}

FULL_COLUMN_ROW <- '\\[{1},{1}\\d+\\:?\\d*\\]{1}|\\[{1}\\d+\\:?\\d*\\,{1}\\]{1}'

evaluateCellNoRewrite <- function(cell) {
  if (notApplicable(cell)) return(cell)
  result <- cell
  try(
    # cell != NA returns NA instead of TRUE/FALSE
    while(!identical(result, eval(parse(text=result)))) {
      proto_name <- sheetNamesNoRewrite
      for (n in sheetNamesNoRewrite) {
        if (!grepl(n, result)) {
          proto_name <- proto_name[proto_name != n]
        }
      }
      # sparklyr must contain only one input slide
      if (length(proto_name) == 1 && any(grepl(proto_name, inputNamesNoRewrite))) {
        proto_cell <- gsub(matchedSheetNames[name], "e", cell)
        function_text <- paste0("`", proto_name, "`", "%>% spark_apply(function(e)", proto_cell, ")")
        pickup_dropoff <- tryCatch({
          collect(eval(parse(text = function_text)))[[1]]
        }, error = function(cond) NA)
        if (length(pickup_dropoff) == 1) {
          nextResult <- pickup_dropoff
        }
      # mean(test.csv[,1:1]) not mean(sheet1[,1:1]) (cell referenced: "sheet1[2:2]")
      } else if (grepl(FULL_COLUMN_ROW, result)
        && length(gregexpr("(", result, fixed=T)[[1]]) == 1
        && length(gregexpr(")", result, fixed=T)[[1]])) {
        nextResult <- tryCatch({
          resultNaRm <- substr(result, 1, nchar(result)-1)
          resultNaRm <- paste0(resultNaRm, ",na.rm=T)")
          resultNaRm <- eval(parse(text=resultNaRm))
          if (!(is.numeric(resultNaRm) && length(resultNaRm) == 1)) {
            # cbind(Intercept=1,fibrinogen=test.csv[,1:1]
            # If other columns are longer, will take in the longer length. Leads to trailing Intercept 1's.
            resultNaRm <- eval(parse(text=result))
          }
          resultNaRm
        }, error = function(cond) NA)
      } else {
        nextResult <- eval(parse(text=result))
      }
      if (is.na(nextResult)) break
      result <- nextResult
      if (length(result) > 1) break
    },
  silent=T)
  if(is.null(result) || is.function(result)) return(cell)
  return(result)
}

EQUATION_FORM_SPLIT <- "\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|(?!\\d),(?!\\d)|\\s|(?<=\\])\\s*\\,"

splitAndReplaceNoRewrite <- function(cell, dataname) {
  # matrix(c(1:9), 3)
  cell <- evaluateCellNoRewrite(cell)
  if (notApplicable(cell) || is.numeric(cell)) return(cell)
  if (is.factor(cell)) {
    return(as.numeric(cell))
  }
  if (!is.null(dataname)) {
    dataNameRegex <- paste0(EQUATION_FORM_SPLIT, "|\\({1}(?!", dataname, ")")
  } else {
    dataNameRegex <- EQUATION_FORM_SPLIT
  }
  stringParts <- strsplit(cell, dataNameRegex, perl=T)[[1]]
  evaluateParts <- sapply(stringParts, evaluateCellNoRewrite, USE.NAMES=F)
  # Some cases of matrix multiplication %*% produces list resulting in evaluateCellNoRewrite working when we don't want it to
  if (identical(typeof(evaluateParts), "list")) {
    evaluateParts <- stringParts
  }
  newCell <- cell
  for (part in 1:length(stringParts)) {
    if (!identical(stringParts[part],"")) {
      newCell <- sub(stringParts[part], evaluateParts[part], newCell, fixed=T)
    }
  }
  evaluateCellNoRewrite(newCell)
}

hasCellReferenceNoRewrite <- function(cell) {
  for (s in sheetNamesNoRewrite) {
    if (grepl(sheet, cell)) {
      return(T)
    }
  }
  return(F)
}

applyNumeric <- function(slide) {
  slide <- eval(parse(text=slide))
  apply(slide, 1:2, function(x) as.numeric(as.character(x)))
}

numerizeAndSplitNoRewrite <- function(cell, original) {
  if (notApplicable(cell)) return(cell)
  # "Sheet1!C3 -- Sheet1!C4 -- Dec-31 or 0.42 or 1+3"
  # "Sheet1!C3 -- Sheet1!C4 + Sheet1!C5 -- 0.43"
  if (is.numeric(cell) || !hasCellReferenceNoRewrite(cell)) {
    return(cell)
  }
  # "Sheet1!C3 + Sheet1!C6 -- 42 or Dec-31+0.1919"
  # "Sheet1!C3 -- Dec-31"
  applyToSheets(function(s, name) {
    name <- sheetNamesNoRewrite[s]
    longitude <- applyNumeric(name)
    # "Sheet1!C3 -- mean(Sheet1$Rating_X) + 0.43"
    if (grepl("$", cell, fixed=T)) {
      eval(call("<<-", as.name(name), as.data.frame(longitude, stringsAsFactors=F)))
    # "Sheet1!C3 -- mean(Sheet1[,2:3]) + 0.43"
    } else {
      eval(call("<<-", as.name(name), longitude))
    }
    # Without cell reference: "mean(Sheet1[2:3,2:2]) + mean(Sheet1[3:4,2:2])"
    # Without cell reference: "Sheet4[1:3,1:3] %*% Sheet4[1:3,2:4]"
    # "mean(Sheet4$Rating_X)" will not work.
    # No header cell reference. In order to accomodate Inputs alongside other cell reference.
    newStoredString <- splitAndReplaceNoRewrite(cell, name)
    # Repeats when sheetNames ["test.csv", "sheet1"] instead of ["sheet1", "test.csv"]
    if (!identical(newStoredString, cell)) return(newStoredString)
  })
  original
}

computeStandardRNoRewrite <- function(cell) {
  if (notApplicable(cell)) return(cell)
  # Matrix multiplication expected to be numeric so cell should not be evaluated prior to applyNumeric
  # Should not evaluateCell prior to splitAndReplaceNoRewrite, only first element will be used
  storedString <- cell
  if (!grepl("%*%", cell)) {
    nextStoredString <- splitAndReplaceNoRewrite(storedString, NULL)
    while(storedString != nextStoredString) {
      storedString <- nextStoredString
      if (grepl("%*%", storedString)) break
      nextStoredString <- splitAndReplaceNoRewrite(storedString, NULL)
    }
  }
  numerizeAndSplitNoRewrite(storedString, cell)
}

configureAndComputeNoRewrite <- function(cell) {
  configureSheetsToMatrixNoRewrite()
  computeStandardRNoRewrite(cell)
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

CELL_REGEX <- "\\[{1}\\d+\\,{1}\\d+\\]{1}"
RANGE_REGEX <- "\\[{1}\\d*\\:?\\d*\\,{1}\\d*\\:?\\d*\\]{1}"

# sheet1[1,], sheet1[,1:1], sheet1[1,2], sheet1[1:2,2], sheet1[1:2,1:2]
appendMatchNumbers <- function(range) {
  matchRange <- regmatches(range, regexpr(RANGE_REGEX, range))
  matchNumbers <- regmatches(matchRange, gregexpr("\\d+", matchRange))[[1]]
  if(length(matchNumbers) == 1 && grepl(FULL_COLUMN_ROW, matchRange)) {
    matchSlide <- sub(FULL_COLUMN_ROW, "", range)
    # sheet1[,2]
    if (grepl("\\[{1},{1}\\d+\\:?\\d*\\]{1}", range)) {
      rowLength <- nrow(eval(parse(text=matchSlide)))
      result <- c(1, rowLength, matchNumbers, matchNumbers)
    # sheet1[2,]
    } else {
      colLength <- ncol(eval(parse(text=matchSlide)))
      result <- c(matchNumbers, matchNumbers, 1, colLength)
    }
  } else if (length(matchNumbers) == 2 && grepl(CELL_REGEX, matchRange)) {
    # sheet[1,2]
    result <- c(matchNumbers[1], matchNumbers[1], matchNumbers[2], matchNumbers[2])
  } else if (length(matchNumbers) == 2 && grepl("\\d+\\:{1}\\d+\\,{1}|\\,{1}\\d+\\:{1}\\d+", matchRange)) {
    # sheet1[2:2,]
    if (grepl("\\d+\\:{1}\\d+\\,{1}", matchRange)) {
      colLength <- ncol(eval(parse(text=matchSlide)))
      result <- c(matchNumbers, 1, colLength)
    # sheet1[,2:2]
    } else {
      rowLength <- nrow(eval(parse(text=matchSlide)))
      result <- c(1, rowLength, matchNumbers)
    }
  } else if (length(matchNumbers) < 3 && grepl("\\d+\\:{1}\\d+\\,{1}\\d+|\\d+\\,{1}\\d+\\:{1}\\d+", matchRange)) {
    # sheet[1:2,2]
    if (grepl("\\d+\\:{1}\\d+\\,{1}\\d+", matchRange)) {
      result <- append(matchNumbers, matchNumbers[3], after=3)
    # sheet[1,1:2]
    } else {
      result <- append(matchNumbers, matchNumbers[1], after=0)
    }
  }
  return(as.numeric(result))
}

isValidParameters <- function(range) {
  # sheet1[1:2,] won't work yet
  matchNumbers <- appendMatchNumbers(range)
  if (length(matchNumbers) != 4) return(F)
  return(T)
}

lattitudeAsDataframe <- function(firstrow, currentLattitude, range) {
  if (firstrow == T) {
    colnames(currentLattitude) <- currentLattitude[1,]
    currentLattitude <- currentLattitude[-1,]
  } else {
    # is Valid Parameters
    if (!isValidParameters(range)) {
      stop("Invalid range.")
    }
    rowLength <- length(eval(parse(text=rangeToMatchSlideQuotes(range)))[,1])
    matchNumbers <- appendMatchNumbers(range)
    colLength <- matchNumbers[4]-matchNumbers[3]+1
    colnames <- vector(mode="character", length=colLength)
    for (i in 1:colLength) {
      letter <- columnToLetter(matchNumbers[3]+i-1)
      colnames[i] <- paste0(letter, matchNumbers[1], ":", letter, matchNumbers[2])
    }
    colnames(currentLattitude) <- colnames
  }
  currentLattitude <- apply(currentLattitude, 1:2, as.numeric)
  as.data.frame(currentLattitude)
}

#
## Compute Spark
#

applyToSheets(function(s, name) {
  currentLattitude <- eval(parse(text=name))
  currentLattitude <- apply(currentLattitude, 1:2, function(x) configureAndComputeNoRewrite(x)[1])
  eval(call("<-", as.name(name), currentLattitude))
})

applyToSheets(function(s, name) {
  currentLattitude <- eval(parse(text=name))
  # Must splitAndReplaceNoRewrite cells to level 1 cell reference. eval(call()) will replace sheets to numeric causing cell reference to return NA
  currentLattitude <- apply(currentLattitude, 1:2, function(x) splitAndReplaceNoRewrite(x, NULL))
  currentLattitude <- apply(currentLattitude, 1:2, function(x) numerizeAndSplitNoRewrite(x, x)[1])
  type <- initialDataNoRewrite[[s]]$type
  rows <- initialDataNoRewrite[[s]]$rows
  returnToInitialDataNoRewrite(rows, currentLattitude)
})

for (s in 1:length(initialDataNoRewrite)) {
  type <- initialDataNoRewrite[[s]]$type
  if (type == "chart" || type == "regression") {
    range <- initialDataNoRewrite[[s]]$range
    firstrow <- initialDataNoRewrite[[s]]$firstrow
    name <- gsub(" ", "_", initialDataNoRewrite[[s]]$name)
    # Range???
    currentLattitude <- lattitudeAsDataframe(firstrow, eval(parse(text=name)), range)
    # Charts
    if (type == "chart") {
      types <- eval(parse(text=initialDataNoRewrite[[s]]$types))
      variablex <- initialDataNoRewrite[[s]]$variablex
      variabley <- initialDataNoRewrite[[s]]$variabley
      if (hasArg(variabley)) {
      	pngChart <- ggplot(currentLattitude, aes_string(variablex, variabley)) + types
      } else {
      	pngChart <- ggplot(currentLattitude, aes_string(variablex)) + types
      }
      ggsave("/tmp/ggplot.png", pngChart)
      initialDataNoRewrite[[s]]$datauri <- base64enc::dataURI(file="/tmp/ggplot.png", mime="image/jpeg")
    # Regressions
    } else if (type == "regression") {
      formulatext <- initialDataNoRewrite[[s]]$formulatext
      # use spark_apply if slide is from input
      if (type == "input") {
      	formulaTextWithE <- sub("currentLattitude", "e", formulaText)
      	completeFormulaText <- paste0("`", name, "`%>%spark_apply(function(e)broom::", formulaTextWithE, ")")
      	currentLattitude <- tryCatch({
      		collect(eval(parse(text=completeFormulaText)))
      	}, error = function(cond) {
      		return('#ERROR!')
      	})
      } else {
      	currentLattitude <- eval(parse(text = formulaText))
      }
      # Save back to initialDataNoRewrite
      if (currentLattitude != '#ERROR!') {
        rows <- initialDataNoRewrite[[s]]$rows
        returnToInitialDataNoRewrite(rows, currentLattitude)
      }
    # Optimizations
    } else if (type == "optimization") {
      # should be after? Accounting for regressions
    }
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
		null=c("null")
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

if (grepl("run", args[[1]])) {
	newPathToFileGCP <- paste(args[[1]], runCounter, sep=" ")
} else {
	newPathToFileGCP <- paste(args[[1]], "run", runCounter, sep=" ")
}

gcs_upload(
	initialDataNoRewrite,
	name=newPathToFileGCP,
	object_function=json_structure)

sparkR.session.stop()

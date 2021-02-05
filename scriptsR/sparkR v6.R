#
### Install Packages
#

install.packages("Rcpp", repos="https://rcppcore.github.io/drat")
install.packages('sparklyr', repos = "http://cran.us.r-project.org")
install.packages("googleCloudStorageR", repos = "http://cran.us.r-project.org")
install.packages("jsonlite", repos = "http://cran.us.r-project.org")
install.packages("ggplot2", repos = "http://cran.us.r-project.org")
install.packages("base64enc", repos = "http://cran.us.r-project.org")
install.packages("broom", repos = "http://cran.us.r-project.org")
devtools::install_github("cloudyr/rdatastore")
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
config <- spark_config()
config$`sparklyr.shell.driver-class-path` <- "/tmp/mysql-connector-java-8.0.20.jar:/tmp/mssql-jdbc-8.4.0.jre8.jar:/tmp/ojdbc8.jar"
sc <- spark_connect(master = "yarn-client", config = config)

#
### Get Database Credentials
#

# Create Google KMS service
library(googleAuthR)
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
library(rdatastore)
Sys.setenv(GOOGLE_APPLICATION_CREDENTIALS = "/tmp/tart-12b0c03bcce1.json")
authenticate_datastore("tart-90ca2")

authUser <- strsplit(
	substr(
		args[[1]],
		6,
		nchar(args[[1]])
	), "\\/")[[1]][1]
credentials <- lookup("connections", authUser)

#
### Load Tables to Spark
#

for (slide in initialDataNoRewrite) {
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
			url <- paste0(
				"jdbc:mysql://",
				host,
				":",
				port,
				"/",
				slide$database,
				"?serverTimezone=UTC")

			eval(call(
				"<-",
				as.name(gsub(" ", "_", slide$name)),
				spark_read_jdbc(
					sc,
					name = gsub(" ", "_", slide$name),
					options = list(
						url = url,
						user = user,
						password = password,
						dbtable = slide$fileName,
					)
				)
			))
		} else if (identical(slide$delimiter, "SQLServer")) {
			url <- paste0(
				"jdbc:sqlserver://",
				host,
				":",
				port)

			eval(call(
				"<-",
				as.name(gsub(" ", "_", slide$name)),
				spark_read_jdbc(
					sc,
					name = gsub(" ", "_", slide$name),
					options = list(
						url = url,
						user = user,
						password = password,
						dbtable = slide$fileName,
						databaseName = slide$database,
						driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
					)
				)
			))
		} else if (identical(slide$delimiter, "OracleDB")) {
			url <- paste0(
				"jdbc:oracle:thin:@",
				host,
				":",
				port,
				":",
				slide$database)

			eval(call(
				"<-",
				as.name(gsub(" ", "_", slide$name)),
				spark_read_jdbc(
					sc,
					name = gsub(" ", "_", slide$name),
					options = list(
						url = url,
						user = user,
						password = password,
						dbtable = slide$fileName,
						driver = "oracle.jdbc.OracleDriver"
					)
				)
			))
		} else {
			pathToFileSpark <- paste0(args[[2]], slide$file)

			eval(call(
				"<-",
				as.name(gsub(" ", "_", slide$name)),
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
	df <- as.data.frame(matrix(
		NA,
		length(data) - 1,
		length(data[[1]])
	))

	for (row in 1:(length(data)-1)) {
		for (column in 1:length(data[[1]])) {
			if (!is.null(data[[row+1]][[column]]$value)) {
				df[row,column] <- (data[[row+1]][[column]]$value)
			}
		}
	}

	for (column in 1:length(data[[1]])) {
		tryCatch({
			colnames(df)[column] <- data[[1]][[column]]
		}, error=function(e){})
	}

	return(df)
}

rawSheetNamesNoRewrite <- character(length(initialDataNoRewrite))

if (length(initialDataNoRewrite) > 0) {
	for (i in 1:length(initialDataNoRewrite)) {
		rawSheetNamesNoRewrite[i] <- initialDataNoRewrite[[i]]$name
	}
}

sheetNamesNoRewrite <- character(length(initialDataNoRewrite))

if (length(initialDataNoRewrite) > 0) {
	for (i in 1:length(initialDataNoRewrite)) {
		sheetNamesNoRewrite[i] <- gsub(" ", "_", initialDataNoRewrite[[i]]$name)
	}
}

LETTERS_REFERENCE <- letters[1:26]

lettersToColumn <- function(letter) {
	column <- 0
	length <- nchar(letter)
	for (i in 1:length) {
		alphabet <- substr(letter,i,i)
		column <- match(tolower(letter), LETTERS_REFERENCE) * 26^(length-i) + column
	}
	return(column)
}

FUNCTIONS_WITH_PREFIX <- c("rnorm(", "rexp(", "rgamma(", "rpois(", "rweibull(", "rcauchy(", "rbeta(", "rt(", "rf(",
	"rbinom(", "rgeom(", "rhyper(", "rlogis(", "rlnorm(", "rnbinom(", "runif(", "rwilcox(")

addPrefixToFunction <- function(cell) {
	for (func in FUNCTIONS_WITH_PREFIX) {
		cell <- gsub(
			func,
			paste0(func, "1,"),
			cell)
	}

	return(cell)
}

translateRForProccess <- function(cell, currentSlide) {
	if (is.null(cell)
		|| is.na(cell)
		|| !identical(substring(cell,1,1),"=")) {

		return(cell)
	}

	match <- regmatches(
		cell,
		gregexpr("[[:upper:]]+\\d*", cell)
	)[[1]]

	removedEqualsCell <- gsub(
		"'",
		"`",
		substring(cell, 2),
		fixed = TRUE)

	for (i in 1:length(initialDataNoRewrite)) {
		removedEqualsCell <- gsub(
			rawSheetNamesNoRewrite[i],
			sheetNamesNoRewrite[i],
			removedEqualsCell)
	}

	if (length(match) == 0) {
		return(removedEqualsCell)
	}

	for (reference in 1:length(match)) {
		column <- lettersToColumn(
			regmatches(
				match[reference],
				regexpr("[[:upper:]]{,2}", match[reference])
			)
		)


		row <- as.numeric(
			regmatches(
				match[reference],
				regexpr("\\d+", match[reference])
			)
		)-1

		if (!identical(reference+1, length(match)-1)) {

			column2 <- lettersToColumn(
				regmatches(
					match[reference+1],
					regexpr("[[:upper:]]{,2}",
					match[reference+1])
				)
			)

			row2 <- as.numeric(
				regmatches(
					match[reference+1],
					regexpr("\\d+", match[reference+1])
				)
			)-1

			if (!grepl("\\d", match[reference])) {
				ref5 <- paste0(
					"[,",
					column, ":", column2,
					"]")

				prefix5 <- paste0(
					"!",
					match[reference], ":", match[reference+1])

				removedEqualsCell <- gsub(
					prefix5,
					ref5,
					removedEqualsCell)

				ref6 <- paste0(
					"`",
					sheetNamesNoRewrite[currentSlide],
					"`[,",
					column, ":", column2,
					"]")

				prefix6 <- paste0(
					match[reference],
					":",
					match[reference+1])

				removedEqualsCell <- gsub(
					prefix6,
					ref6,
					removedEqualsCell)
			}

			ref4 <- paste0(
				"[",
				row, ":", row2,
				",",
				column, ":", column2,
				"]")

			prefix4 <- paste0(
				"!",
				match[reference],
				":",
				match[reference+1])

			removedEqualsCell <- gsub(
				prefix4,
				ref4,
				removedEqualsCell)

			ref3 <- paste0(
				"`", sheetNamesNoRewrite[currentSlide], "`[",
				row, ":", row2,
				",",
				column, ":", column2,
				"]")

			prefix3 <- paste0(
				match[reference],
				":",
				match[reference+1])

			removedEqualsCell <- gsub(
				prefix3,
				ref3,
				removedEqualsCell)
		}

		if (grepl("\\d", match[reference])) {
			ref <- paste0(
				"[",
				row, ",", column,
				"]")
			prefix <- paste0("!", match[reference])
			removedEqualsCell <- gsub(
				prefix,
				ref,
				removedEqualsCell)

			ref2 <- paste0(
				"`", sheetNamesNoRewrite[currentSlide], "`[",
				row, ",", column,
				"]")
			prefix2 <- match[reference]
			removedEqualsCell <- gsub(
				prefix2,
				ref2,
				removedEqualsCell)
		}
	}
	return(addPrefixToFunction(removedEqualsCell))
}

evaluateCellNoRewrite <- function(cell) {
	if (is.null(cell)
		|| is.na(cell)
		|| cell == "") {

		return(cell)
	}

	result <- cell
	try(
		while(!identical(result,eval(parse(text = result)))) {
			if (is.na(eval(parse(text = result)))) {
				break
			}

			result <- eval(parse(text = result))

			if (length(result) > 1) {
				break
			}
		}, silent = TRUE
	)

	if(is.null(result) || is.function(result)) {
		return(cell)
	}

	return(result)
}

# Rewritten function from app.R, different from splitAndReplace
splitAndReplaceNoRewrite <- function(cell) {
	if (is.null(cell)
		|| is.na(cell)
		|| cell == ""
		|| is.numeric(cell)) {
		return(cell)
	}

	stringParts <- strsplit(
		cell,
		EQUATION_SPLIT
	)[[1]]

	evaluateParts <- sapply(
		stringParts,
		evaluateCell,
		USE.NAMES=FALSE)

	# Some cases of matrix multiplication %*% produces list resulting in evaluateCell working when we don't want it to
	if (identical(typeof(evaluateParts), "list")) {
		evaluateParts <- stringParts
	}

	newCell <- cell
	for (part in 1:length(stringParts)) {
		if(!identical(stringParts[part],"")) {
			newCell <- sub(
				stringParts[part],
				evaluateParts[part],
				newCell,
				fixed = TRUE)
		}
	}
	return(evaluateCell(newCell))
}

computeSparklyR <- function(cell) {
	# find out which Sheet is used in the cell
	if (is.null(cell)
		|| is.na(cell)
		|| identical(cell, "")) {
		return(cell)
	}

	cell <- evaluateCellNoRewrite(cell)
	if (is.numeric(cell)) {
		return(cell)
	}

	# We expect only one name to be found due to splitAndReplace
	for (name in 1:length(sheetNamesNoRewrite)) {
		# Return if it isn't a Spark input
		# if (!identical(initialDataNoRewrite[[name]]$type, "input")) return(cell)

		proto_name <- regmatches(
			cell,
			regexpr(sheetNamesNoRewrite[name], cell))

		proto_cell <- gsub(
			sheetNamesNoRewrite[name],
			"e",
			cell)

		# Break loop when sheet name is found
		if (length(proto_name) != 0) {
			break
		}
	}

	if (length(proto_name) == 0) {
		return(cell)
	}

	function_text <- paste0(
		"`", proto_name, "`",
		"%>% spark_apply(function(e)",
		proto_cell,
		")")

	pickup_dropoff_tbl <- tryCatch({
		eval(parse(text = function_text))
	}, error = function(cond) {
		return('#ERROR!')
	})

	if (identical(pickup_dropoff_tbl, '#ERROR!')) {
		return(pickup_dropoff_tbl)
	}

	pickup_dropoff <- collect(pickup_dropoff_tbl)
	if (length(pickup_dropoff[[1]]) > 1) {
		return(cell)
	}

	return(pickup_dropoff[[1]])
}

splitAndReplace <- function(cell) {
	if (is.null(cell)
		|| is.na(cell)
		|| identical(cell, "")
		|| is.numeric(cell)) {

		return(cell)
	}

	# Split first because spark_apply doesn't work with operators(+,-,/,*)
	stringParts <- strsplit(
		cell,
		"\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%|\\(|\\)"
	)[[1]]

	evaluateParts <- simplify2array(lapply(
		stringParts,
		computeSparklyR))

	newCell <- cell
	for (part in 1:length(stringParts)) {
		if(!identical(stringParts[part],"")) {
			newCell <- sub(
				stringParts[part],
				evaluateParts[part],
				newCell,
				fixed = TRUE)
		}
	}

	if (grepl('#ERROR!', newCell)) {
		return('#ERROR!')
	}

	newCell <- evaluateCellNoRewrite(newCell)

	if (is.numeric(newCell)) {
		return(newCell)
	}

	# Do over because some scenarios aren't covered
	stringParts <- strsplit(
		newCell,
		"\\+|\\-|\\*|\\/|\\^|\\%%|\\%/%"
	)[[1]]

	evaluateParts <- simplify2array(lapply(
		stringParts,
		computeSparklyR))

	for (part in 1:length(stringParts)) {
		if(!identical(stringParts[part],"")) {
			newCell <- sub(
				stringParts[part],
				evaluateParts[part],
				newCell,
				fixed = TRUE)
		}
	}

	return(evaluateCellNoRewrite(newCell))
}

substituteMatrix <- function(cell) {
	if (is.null(cell)
		|| is.na(cell)
		|| cell == ""
		|| is.numeric(cell)
		|| !grepl(":", cell)) {

		return(cell)
	}

	proto_matrix <- sub(
		"\\[{1}\\d+\\:?\\d*\\,{1}\\d+\\:?\\d*\\]{1}",
		"",
		cell)

	proto_matrix <- gsub(
		"`",
		"",
		proto_matrix)

	return(proto_matrix)
}

# Rewritten function from app.R, different from splitAndReplace
splitAndReplaceMatrix <- function(cell) {
	if (is.null(cell)
		|| is.na(cell)
		|| cell == ""
		|| is.numeric(cell)) {
		return(cell)
	}

	stringParts <- strsplit(
		cell,
		EQUATION_SPLIT
	)[[1]]

	matrixParts <- stringParts[grepl(":", stringParts)]

	if (length(matrixParts) > 0) {
		matrixParts <- simplify2array(lapply(
			matrixParts,
			substituteMatrix))
	}

	for (proto_matrix in matrixParts) {
		if (exists(proto_matrix)) {
			currentLattitude <- eval(parse(text = proto_matrix))
			currentLattitude <- as.data.frame(
				currentLattitude,
				stringsAsFactors = FALSE)

			currentLattitudeColumns <- colnames(currentLattitude)

			currentLattitude <- as.data.frame(
				lapply(
					currentLattitude,
					function(x) as.numeric(as.character(x))
				), stringsAsFactors = FALSE)

			colnames(currentLattitude) <- currentLattitudeColumns

			eval(call(
				"<-",
				as.name(proto_matrix),
				currentLattitude))
		}
	}

	result <- tryCatch({
		eval(parse(text=cell))
	}, error = function(cond) {
		return('#ERROR!')
	})

	return(result)
}

compareMissingValues <- function(currentLattitude, currentSlide) {
	for (row in 1:nrow(currentLattitude)) {
		for (column in 1:ncol(currentLattitude)) {

			rawCell <- currentSlide[row,column]
			cell <- currentLattitude[row,column]

			if (is.null(rawCell)
				|| is.na(rawCell)
				|| rawCell == ""
				|| !grepl('#ERROR!', cell)) {

				next
			}

			storedString <- splitAndReplaceNoRewrite(
				evaluateCellNoRewrite(rawCell)
			)

			storedString <- as.numeric(as.character(
				splitAndReplaceMatrix(storedString)
			))

			if (!is.na(storedString) && length(storedString) > 0) {
				currentLattitude[row,column] <- storedString
			}
		}
	}
	return(currentLattitude)
}

saveBackToJsonFromDataFrame <- function(dataframe, json) {
	for (row in 1:(length(json)-1)) {
		for (column in 1:length(json[[1]])) {

			if (!is.null(json[[row+1]][[column]]$value)
				&& startsWith(json[[row+1]][[column]]$value, "=")) {

				json[[row+1]][[column]]$value <- toString(dataframe[row,column][[1]])
			}
		}
	}
	return(json)
}

#
## Function Required for Plot and Regression
#

library(ggplot2)
plot <- function(chartdata) {
	variablex <- chartdata$variablex
	variabley <- chartdata$variabley
	type <- chartdata$type
	name <- chartdata$name

	if (exists(gsub(" ", "_", name))) {
		name <- gsub(
			" ",
			"_",
			name)
	}

	currentLattitude <- as.data.frame(
		eval(parse(
			text = paste0("`", name, "`")
		)), stringsAsFactors = FALSE)

	currentLattitudeColumns <- colnames(currentLattitude)
	currentLattitude <- as.data.frame(
		lapply(
			currentLattitude,
			function(x) as.numeric(as.character(x))
		), stringsAsFactors = FALSE)

	colnames(currentLattitude) <- currentLattitudeColumns

	if (hasArg(variabley)) {

		pngChart <- ggplot(
			currentLattitude,
			aes_string(variablex, variabley)
		) + eval(parse(text = type))

	} else {

		pngChart <- ggplot(
			currentLattitude,
			aes_string(variablex)
		) + eval(parse(text = type))

	}

	ggsave("/tmp/ggplot.png", pngChart)

	return(base64enc::dataURI(
		file="/tmp/ggplot.png",
		mime="image/jpeg"))
}

library(broom)
regressionNormal <- function(name, formulatext) {

	if (exists(gsub(" ", "_", name))) {
		name <- gsub(" ", "_", name)
	}

	currentLattitude <- as.data.frame(
		eval(parse(
			text = paste0("`", name, "`")
		)), stringsAsFactors = FALSE)

	currentLattitudeColumns <- colnames(currentLattitude)

	currentLattitude <- as.data.frame(
		lapply(
			currentLattitude,
			function(x) as.numeric(as.character(x))
		), stringsAsFactors = FALSE)

	colnames(currentLattitude) <- currentLattitudeColumns

	currentLattitude <- eval(parse(text = formulaText))

	return(currentLattitude)
}

regressionSparklyR <- function(name, formulatext) {
	if (exists(gsub(" ", "_", name))) {
		name <- gsub(
			" ",
			"_",
			name)
	}
	formulaTextWithE <- sub(
		"currentLattitude",
		"e",
		formulaText)

	completeFormulaText <- paste0(
		"`",
		name,
		"`%>%spark_apply(function(e)broom::",
		formulaTextWithE,
		")")

	pickup_dropoff_tbl <- tryCatch({
		eval(parse(
			text = completeFormulaText
		))
	}, error = function(cond) {
		return('#ERROR!')
	})

	if (identical(pickup_dropoff_tbl, '#ERROR!')) {
		return(pickup_dropoff_tbl)
	}

	return(collect(pickup_dropoff_tbl))
}

regression <- function(regressiondata) {
	regressionSheetname <- regressiondata$name
	formulatext <- regressiondata$formulatext

	# Find if sheet being used is a Spark Input or regular table
	for (name in 1:length(sheetNamesNoRewrite)) {
		if (identical(initialDataNoRewrite[[name]]$name, regressionSheetname)) {

			if (identical(initialDataNoRewrite[[name]]$type, "input")) {
				regressionParsed <- regressionSparklyR(
					regressionSheetname,
					formulatext)

				return(regressionParsed)
			} else {
				regressionParsed <- regressionNormal(
					regressionSheetname,
					formulatext)

				return(regressionParsed)
			}
			break
		}
	}
}

saveBackToJsonFromRegression <- function(dataframe, json) {
	for (row in 1:nrow(dataframe)) {
		for (column in 1:ncol(dataframe)) {

			if (!is.null(json[[row+1]][[column]]$value)) {
				json[[row+1]][[column]]$value <- toString(
					dataframe[row,column][[1]]
				)
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
		longitudeTemporaryData <- listToDataframe(
			initialDataNoRewrite[[slide]]$data)

		longitudeTemporaryData <- apply(
			longitudeTemporaryData,
			1:2,
			translateRForProccess)

		eval(call(
			"<-",
			as.name(initialDataNoRewrite[[slide]]$name),
			longitudeTemporaryData
		))
	}
}

for (slide in 1:length(initialDataNoRewrite)) {
	if (identical(initialDataNoRewrite[[slide]]$type, "sheet")) {

		currentSlide <- eval(parse(
			text = initialDataNoRewrite[[slide]]$name
		))

		longitudeTemporaryData <- apply(
			currentSlide,
			1:2,
			splitAndReplace)

		eval(call(
			"<-",
			as.name(initialDataNoRewrite[[slide]]$name),
			longitudeTemporaryData
		))
	}
}

# Compare longitudeTemporaryData with ALL slides to recalculate #ERROR! where "mean(Sheet1[2:3,2:2]) + mean(Sheet1[3:4,2:2]) -- mean(Sheet2[,2:2]), mean(Sheet2[,3:3])"
for (slide in 1:length(initialDataNoRewrite)) {
	if (identical(initialDataNoRewrite[[slide]]$type, "sheet")) {

		currentSlide <- listToDataframe(initialDataNoRewrite[[slide]]$data)

		currentSlide <- apply(
			currentSlide,
			1:2,
			translateRForProccess)

		longitudeTemporaryData <- eval(parse(
			text = initialDataNoRewrite[[slide]]$name
		))

		longitudeTemporaryData <- compareMissingValues(
			longitudeTemporaryData,
			currentSlide)

		initialDataNoRewrite[[slide]]$data <- saveBackToJsonFromDataFrame(
			longitudeTemporaryData,
			initialDataNoRewrite[[slide]]$data)

	}
}

# Chart and Regress after all formulas in sheet have been calculated
for (slide in 1:length(initialDataNoRewrite)) {
	if (identical(initialDataNoRewrite[[slide]]$type, "chart")) {

		charturi <- plot(initialDataNoRewrite[[slide]]$data)
		initialDataNoRewrite[[slide]]$data$datauri <- charturi

	} else if (identical(initialDataNoRewrite[[slide]]$type, "regression")) {

		regressionParsed <- regression(
			initialDataNoRewrite[[slide]]$regression)

		if (!identical(regressionParsed, '#ERROR!')) {
			initialDataNoRewrite[[slide]]$data <- saveBackToJsonFromRegression(
				regressionParsed,
				initialDataNoRewrite[[slide]]$data)
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
		pretty = FALSE,
		auto_unbox = TRUE,
		null = c("null"))
}

substrRight <- function(x, n){
	substr(
		x,
		nchar(x)-n+1,
		nchar(x))
}
runCounter <- as.numeric(
	substrRight(
		max(grep(
			args[[1]],
			objects$name,
			value=TRUE
		)),
	1))

if (is.na(runCounter)) {
	runCounter <- 1
} else {
	runCounter <- runCounter + 1
}

if (grepl("run", args[[1]])) {

	newPathToFileGCP <- paste(
		args[[1]],
		runCounter,
		sep = " ")

} else {

	newPathToFileGCP <- paste(
		args[[1]],
		"run",
		runCounter,
		sep = " ")

}

gcs_upload(
	initialDataNoRewrite,
	name=newPathToFileGCP,
	object_function=json_structure)

sparkR.session.stop()

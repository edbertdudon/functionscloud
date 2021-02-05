needs(tibble)
attach(input[[1]])

parseString <- apply(currentSheet, 1:2, function(x) tryCatch({eval(parse(text=x))}, error = function(e) {return(x)}))
as_tibble(parseString)
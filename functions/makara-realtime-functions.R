# Packages: rjson, httr
readRealtimeTrackingSmart <- function(secrets=NULL, token, id) {
    if(!is.null(secrets)) {
        secrets <- readSmartSecrets(secrets)
        token <- secrets$smart_key
        id <- secrets$rt_tracking_id
    }
    header <- add_headers(Authorization = paste0('Bearer ', token))
    base <- 'https://api.smartsheetgov.com/2.0/sheets'
    apiData <- GET(url=paste0(base, '/', id), config=header)
    data <- smartToDf(apiData)
    data
}

readSmartSecrets <- function(x) {
    if(is.list(x) &&
       'smart_key' %in% names(x)) {
        return(x)
    }
    text <- readLines(x)
    text <- strsplit(text, ':')
    text <- lapply(text, function(t) {
        base <- gsub("\\s|'", '', t)
        out <- list(base[2])
        names(out) <- base[1]
        out
    })
    text <- unlist(text, recursive=FALSE)
    text
}

smartToDf <- function(x) {
    data <- fromJSON(rawToChar(x$content))
    cols <- sapply(data$columns, function(x) x$title)
    rows <- lapply(data$rows, function(r) {
        one <- lapply(r$cells, function(c) {
            val <-as.character(c$value)
            # bind_rows doenst like len 0 parts
            if(length(val) == 0) {
                return(NA)
            }
            val
        })
        names(one) <- cols
        one
    })
    result <- bind_rows(rows)
    result
}

readRealtimeDetection <- function(x) {
    data <- read.csv(x, stringsAsFactors = FALSE)
    data$UTC <- format(as.character(x$datetime_utc),
                       format='%Y%m%d%H%M%S',
                       tz='UTC'
    )
    data
}

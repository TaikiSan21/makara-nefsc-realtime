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

baseDetectionLoader <- function(x) {
    # these are missing analyst column
    switch(basename(x$file[1]),
           'WHOI_SBNMS_201412_WE04_detectiondata.csv' = {
               x$analyst <- ''
           },
           'WHOI_SBNMS_201412_WE10_detectiondata.csv' = {
               x$analyst <- ''
           }
    )
    x$deployment_code <- gsub('_detectiondata\\.csv$', '', x$file)
    x$end <- as.POSIXct(as.character(x$datetime_utc),
                        format='%Y%m%d%H%M%S',
                        tz='UTC'
    )
    x <- arrange(x, end)
    x$start <- x$end - 15 * 60 # start 15 min before
    endEarly <- x$end[1:(nrow(x)-1)] > x$start[2:nrow(x)]
    if(any(endEarly)) {
        ix <- which(endEarly)
        x$start[ix+1] <- x$end[ix]
    }
    # check for times where analyst field seems to be a comment
    anaComment <- grepl('\\+', x$analyst)
    # some comments not caught by this are single word "nada"
    justNada <- x$analyst == 'nada'
    anaComment <- anaComment | justNada
    if(any(anaComment)) {
        noNote <- x$notes[anaComment] == ''
        x$notes[anaComment][noNote] <- x$analyst[anaComment][noNote]
        x$notes[anaComment][!noNote] <- paste0(x$notes[anaComment][!noNote],
                                               '; ',
                                               x$analyst[anaComment][!noNote])
    }
    speciesNames <- c(
        'fin',
        'sei', 
        'right',
        'humpback',
        'blue',
        'beluga',
        'killer',
        'bearded',
        'bowhead',
        'walrus',
        'airgun',
        'other',
        'brydes'
    )
    # x <- fixRTAnalyst(x)
    x <- pivot_longer(x,
                      cols=any_of(speciesNames),
                      names_to = 'species',
                      values_to = 'presence'
    )
    x
}

formatDetectionData <- function(x) {
    colMap <- list(
        'start' = 'detection_start_datetime',
        'end' = 'detection_end_datetime',
        'species' = 'detection_sound_source_code',
        'lat' = 'detection_latitude',
        'lon' = 'detection_longitude',
        'presence' = 'detection_result_code'
    )
    spMap <- list(
        'fin' =  'FIWH',
        'sei' = 'SEWH', 
        'right' = 'RIWH',
        'humpback' = 'HUWH',
        'blue' = 'BLWH',
        'beluga' = 'BELU',
        'killer' = 'KIWH',
        'bearded' = 'BESE',
        'bowhead' = 'BOWH',
        'walrus' = 'WALR',
        'airgun' = 'RV-G', #maybe UNAN ? dont see one CHECK
        'other' = 'OTHE', # CHECK
        'brydes' = 'BRWH'
    )
    callMap <- list(
        'FIWH' = 'FIWH_20HZ',
        'BLWH' = 'BLWH_SONG',
        'RIWH' = 'RW_UPCALL',
        'HUWH' = 'HUWH_MIX',
        'SEWH' = 'SEWH_DS80HZ', # END FROM bott mount
        'BELU' = 'OD_MIX',
        'KIWH' = 'OD_MIX',
        'BESE' = 'BESE_MIX',
        'BOWH' = 'BOWH_MIX',
        'WALR' = 'WALR_MIX',
        'RV-G' = 'TEMP',
        'OTHE' = 'TEMP',
        'BRWH' = 'BRWH_MIX'
    )
    resultMap <- list(
        'absent' = 'NOT_DETECTED',
        'maybe' = 'POSSIBLY_DETECTED',
        'present' = 'DETECTED'
        )
    names(x) <- myRenamer(names(x), colMap)
    x$detection_sound_source_code <- myRenamer(x$detection_sound_source_code, map=spMap)
    x$detection_call_type_code <- myRenamer(x$detection_sound_source_code, map=callMap)
    x$detection_result_code <- myRenamer(x$detection_result_code, map=resultMap)
    x
}

# correct should be name.name
# dont need - already made analysis_analysts column in smort
# fixRTAnalyst <- function(x) {
#     vals <- unique(x$analyst)
#     hasDot <- grepl('\\.', vals)
#     isBlank <- vals == ''
#     if(all(isBlank)) {
#         warning('No analyst in deployment ', x$deployment_code[1])
#         return(x)
#     }
#     if(length(vals) == 2 &&
#        any(isBlank) &&
#        any(hasDot)) {
#         vals <- vals[hasDot]
#         hasDot <- TRUE
#         x$analyst <- vals
#     }
#     if(any(!hasDot)) {
#         warning('Unexpected analyst names in deployment ',
#                 x$deployment_code[1],
#                 '(', printN(vals[!hasDot]), ')'
#         )
#     }
#     x
# }

sameNameCombiner <- function(x) {
    if(is.character(x)) {
        x <- lapply(x, readRealtimeDetection)
    }
    allNames <- lapply(x, colnames)
    ixVec <- rep(0, length(allNames))
    distNames <- list()
    for(i in seq_along(allNames)) {
        if(i==1) {
            distNames <- list(allNames[[i]])
        }
        isIn <- unlist(lapply(distNames, function(y) setequal(allNames[[i]], y)))
        if(!any(isIn)) {
            distNames[[length(distNames)+1]] <- allNames[[i]]
            ixVec[i] <- length(distNames)
        } else {
            ixVec[i] <- which(isIn)
        }
    }
    distNames
    combOld <- vector('list', length=max(ixVec))
    for(i in seq_along(combOld)) {
        combOld[[i]] <- bind_rows(x[ixVec == i])
    }
    combOld
}

fillNAFromOther <- function(x, to_columns, 
                            y, from_columns, 
                            by, 
                            comment_columns=NULL,
                            comment,
                            create_missing=FALSE, 
                            verbose=FALSE) {
    if(length(to_columns) != length(from_columns)) {
        stop('to_columns and from_columns must have same length')
    }
    if(!is.null(comment_columns) &&
       (length(comment_columns) != length(to_columns) ||
        length(comment_columns) != length(comment))) {
        stop('comment_columns and comment must have same length as to_columns')
    }
    byDNE <- character(0)
    for(c in seq_along(to_columns)) {
        if(!to_columns[c] %in% names(x)) {
            if(isFALSE(create_missing)) {
                if(verbose) {
                    message('Column ', to_columns[c], ' not in data')
                    next
                }
            } else if(isTRUE(create_missing)) {
                x[[to_columns[c]]] <- NA
                if(verbose) {
                    message('Missing column ', to_columns[c], ' created')
                }
            }
        }
        isNA <- is.na(x[[to_columns[c]]])
        if(!any(isNA)) {
            if(verbose) {
                message('No NA vals for column ', to_columns[c])
            }
            next
        }
        nFilled <- 0
        charCol <- is.character(x[[to_columns[c]]])
        for(ix in which(isNA)) {
            if(!x[[by]][ix] %in% y[[by]]) {
                byDNE <- c(byDNE, x[[by]][ix])
                next
            }
            newVal <- y[[from_columns[c]]][y[[by]] == x[[by]][ix]]
            if(inherits(newVal, 'POSIXct') &&
               charCol) {
                newVal <- psxTo8601(newVal)
            }
            if(!is.na(newVal)) {
                nFilled <- nFilled + 1
            }
            x[[to_columns[c]]][ix] <- newVal
            if(!is.null(comment_columns) &&
               !is.na(comment[c])) {
                x[[comment_columns[c]]][ix] <- combineComment(x[[comment_columns[c]]][ix], 
                                                          comment[c],
                                                          sep=';')
            }
            
        }
        if(verbose) {
            message('Filled ', nFilled, ' values out of ',
                    sum(isNA), ' NA in column ', to_columns[c])
        }
    }
    if(length(byDNE) > 0) {
        byDNE <- unique(byDNE)
        warning(length(byDNE), ' "', by, '"s (',
                printN(byDNE), 
                ') did not exist in the filling',
                ' dataset, NAs were not filled')
    }
    x
}

combineComment <- function(x, y, sep=';') {
    if(is.na(x) | x == '') {
        return(y)
    }
    paste0(x, sep, y)
}

detToSummary <- function(x, by=NULL) {
    if(!is.null(by)) {
        return(
            bind_rows(lapply(split(x, x[[by]]), function(y) {
                result <- detToSummary(y, by=NULL)
                result[[by]] <- y[[by]][1]
                result
            }))
        )
    }
    x <- arrange(x, start)
    lats <- x$lat[!is.na(x$lat)]
    lons <- x$lon[!is.na(x$lon)]
    start <- min(x$start)
    end <- max(x$end)
    list(
        firstLat = lats[1],
        lastLat = lats[length(lats)],
        firstLong = lons[1],
        lastLong = lons[length(lats)],
        start = start,
        end = end
    )
}

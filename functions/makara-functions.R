# Generic Makara Functions ----
# Packages: dplyr, tidyr, lubridate, makaraValidatr

## Updating code ----
# uncomment this chunk to force janky single-file update of my common functions
# updateFunctions <- download.file('https://api.github.com/repos/TaikiSan21/makaraHelpers/contents/R/makara-functions.R',
#                      destfile = 'functions/makara-functions.R',
#                      method='libcurl',
#                      headers=c('Accept'= 'application/vnd.github.v3.raw'),
#                      extra='-O -L')

## Functions ----
# map is list of form old=new
# renames either values in a vector or names of dataframe
myRenamer <- function(x, map) {
    if(is.data.frame(x)) {
        names(x) <- myRenamer(names(x), map)
        return(x)
    }
    # check if map is obviously backwards and fix it
    if(!any(x %in% names(map)) &&
       any(x %in% unlist(map))) {
        newMap <- as.list(names(map))
        names(newMap) <- unlist(map)
        map <- newMap
    }
    for(val in names(map)) {
        if(val %in% x) {
            x[x == val] <- map[[val]]
        }
    }
    x
}

# combines a bunch of columns into one, optionally adding a prefix
# to each column entry if it is not NA
combineColumns <- function(x, into, columns, prefix=NULL, sep='; ', warnMissing=TRUE) {
    missing <- !columns %in% names(x)
    if(all(missing)) {
        warning('None of the column(s) ',
                printN(columns),
                ' were present in data, cannot combine,')
        return(x)
    }
    if(sum(missing) > 0 && isTRUE(warnMissing)) {
        warning(sum(missing), ' column(s) to combine were not present in data (',
                printN(columns[missing]), ')')
    }
    columns <- columns[!missing]
    if(!is.null(prefix) &&
       length(prefix) != length(missing)) {
        warning('Must provide equal number of prefixes and columns')
        prefix <- NULL
    }
    if(!is.null(prefix)) {
        prefix <- prefix[!missing]
        for(i in seq_along(columns)) {
            x[[columns[i]]] <- if_else(is.na(x[[columns[i]]]) | x[[columns[i]]] == '', NA_character_, 
                                       paste0(prefix[i], x[[columns[i]]]))
        }
    }
    x <- unite(x, !!into, any_of(c(into, columns)), sep=sep, na.rm=TRUE)
    x
}

# pretty printing helper to print number of items in a list
# n is a cutoff of max to show at once
printN <- function(x, n=6, collapse=', ') {
    nItems <- length(x)
    if(nItems == 0) {
        return('')
    }
    if(nItems > n) {
        x <- c(x[1:n], paste0('... (', nItems-n, ' more not shown)'))
    }
    paste0(paste(x, collapse=collapse))
}

# formats a POSIXct object to 8601 format
psxTo8601 <- function(x) {
    if(is.character(x)) {
        return(x)
    }
    if(!inherits(x, 'POSIXct')) {
        warning('Must be POSIXct or character')
        return(x)
    }
    if(tz(x) != 'UTC') {
        warning('Non-UTC timezone not yet supported')
    }
    format(x, format='%Y-%m-%dT%H:%M:%SZ')
}

# create a POSIXct or full datetime character from separate date
# and time character columns
formatDatetime <- function(date, time, warn=TRUE, type=c('char', 'posix')) {
    date[is.na(date)] <- ''
    time[is.na(time)] <- ''
    datetime <- paste0(date, ' ', time)
    bothMissing <- datetime == ' '
    if(any(bothMissing) && isTRUE(warn)) {
        warning(sum(bothMissing), ' dates did not have a date or time component')
    }
    datetime[bothMissing] <- NA
    datetime <- parse_date_time(
        datetime,
        orders=c('%Y-%m-%d %H:%M:%S',
                 '%Y/%m/%d %H:%M:%S',
                 '%m/%d/%Y %H:%M:%S'),
        truncated = 3,
        tz='UTC')
    noParse <- is.na(datetime) & !bothMissing
    if(any(noParse) && isTRUE(warn)) {
        warning(sum(noParse), ' dates could not be parsed')
    }
    type <- match.arg(type)
    if(type == 'char') {
        return(psxTo8601(datetime))
    }
    datetime
}

# Helper for tracking warnings in various checking functions
addWarning <- function(x, deployment, table, type, message) {
    if('warnings' %in% names(x)) {
        x$warnings <- addWarning(x$warnings, deployment=deployment, table=table,
                                 type=type, message=message)
        return(x)
    }
    bind_rows(x, list(deployment=deployment, table=table, type=type, message=message))
}

# Check data against templates for missing mandatory data and other issues
# x is a list with each table 
# templates comes from `formatBasicTemplates`
# mandatory is constant list 
# ncei flag is whether to check columns that are only mandatory for NCEI
# dropEmpty is flag whether to drop empty non-mandatory columns from output
checkMakTemplate <- function(x, templates, ncei=FALSE, dropEmpty=FALSE) {
    result <- templates[names(x)]
    onlyNotLost <- c('recording_start_datetime',
                     'recording_duration_secs',
                     'recording_interval_secs',
                     'recording_sample_rate_khz',
                     'recording_n_channels',
                     'recording_timezone')
    col_defs <- makaraValidatr::column_definitions
    mandatory <- lapply(col_defs, function(x) {
        list(
            'always' = x$name[x$required],
            'ncei' = x$name[x$required_ncei]
        )
    })
    warns <- vector('list', length=0)
    uniqueConditions <- lapply(col_defs, function(d) {
        vals <- d$unique_by[!is.na(d$unique_by)]
        if(length(vals) == 0) {
            return(NULL)
        }
        vals <- strsplit(vals, ',')[[1]]
        vals <- c(d$name[!is.na(d$unique_by)], vals)
        vals
    })
    for(n in names(x)) {
        thisTemp <- templates[[n]]
        thisMand <- mandatory[[n]]$always
        thisNcei <- mandatory[[n]]$ncei
        thisData <- x[[n]]
        # special case detections no mand locals
        if(n == 'detections') {
            if(!'localization_latitude' %in% names(thisData) ||
               all(is.na(thisData$localization_latitude))) {
                thisMand <- thisMand[thisMand != 'localization_method_code']
            }
            if(!'localization_depth_m' %in% names(thisData) ||
               all(is.na(thisData$localization_depth_m))) {
                thisMand <- thisMand[thisMand != 'localization_depth_method_code']
            }
        }
        # checking that i didnt goof mandatory names
        missMand <- !thisMand  %in% names(thisTemp)
        if(any(missMand)) {
            warning(sum(missMand), ' misspelled mandatory names for ', n,
                    printN(thisMand[missMand]))
        }
        if(isTRUE(ncei)) {
            missNcei <- !thisNcei %in% names(thisTemp)
            if(any(missNcei)) {
                warning(sum(missNcei), 'misspelled ncei names for ', n, 
                        printN(thisNcei[missNcei]))
            }
            missNcei <- !thisNcei %in% names(thisData)
            if(any(missNcei)) {
                warns <- addWarning(warns, deployment='All', table=n, type='Missing NCEI Columns', 
                                    message=paste0('Mandatory NCEI columns ', 
                                                   printN(thisNcei[missNcei], Inf), ' are missing'))
            }
        }
        wrongNames <- !names(thisData) %in% names(thisTemp)
        if(sum(wrongNames) > 0) {
            warns <- addWarning(warns, deployment='All', table=n, type='Extra Columns',
                                message=paste0('Extra columns ', printN(names(thisData)[wrongNames], Inf),
                                               ' are present'))
            thisData <- thisData[!wrongNames]
        }
        # check that codes are unique if they should be
        
        uniqueCols <- uniqueConditions[[n]]
        if(!is.null(uniqueCols)) {
            checkDupeDeps <- thisData %>% 
                summarise(dupe = n() > 1, .by=all_of(uniqueCols))
            dupeDeps <- checkDupeDeps$dupe
            codePrint <- paste0('"', paste0(uniqueCols, collapse='-'), '"')
            if(any(dupeDeps)) {
                warns <- addWarning(warns, 
                                    deployment=checkDupeDeps$deployment_code[dupeDeps],
                                    table=n,
                                    type=paste0('Duplicated ', codePrint),
                                    message=paste0(codePrint, 
                                                   checkDupeDeps$deployment_code[dupeDeps],
                                                   ' has multiple entries in ', n)
                )
            }
        }
        # check that mandatory columns atually exist
        missMand <- !thisMand  %in% names(thisData)
        if(any(missMand)) {
            warns <- addWarning(warns, deployment='All', table=n, type='Missing Columns', 
                                message=paste0('Mandatory columns ', 
                                               printN(thisMand[missMand], Inf), ' are missing'))
        }
        # Fix time columns
        timeCols <- grep('datetime', names(thisData), value=TRUE)
        for(t in timeCols) {
            # no problems converting if its already posix
            if(inherits(thisData[[t]], 'POSIXct')) {
                thisData[[t]] <- psxTo8601(thisData[[t]])
                next
            } 
            alreadyNa <- is.na(thisData[[t]]) | thisData[[t]] == ''
            times <- makeValidTime(thisData[[t]][!alreadyNa])
            goodTime <- !is.na(times)
            thisData[[t]][!alreadyNa][goodTime] <- times[goodTime]
            if(any(!goodTime)) {
                warns <- addWarning(warns, deployment=thisData$deployment_code[!alreadyNa][!goodTime],
                                    type='Invalid Time',
                                    table=n,
                                    message=paste0("Time '", thisData[[t]][!alreadyNa][!goodTime], "' in column '",
                                                   t, "'could not be converted'"))
            }
            
        }
        # check that values in mandatory columns are not NA or ''
        for(m in thisMand[!missMand]) {
            if(is.character(thisTemp[[m]])) {
                blankChar <- !is.na(thisData[[m]]) & thisData[[m]] == ''
                if(any(blankChar)) {
                    # warns <- addWarning(warns, deployment=thisData$deployment_code[blankChar],
                    #                     type='Blank Characters in Mandatory Field',
                    #                     table=n,
                    #                     message=paste0("Character values in mandatory column '",
                    #                                    m, "' are empty, will be replaced with NA"))
                }
                thisData[[m]][blankChar] <- NA
            }
            if(m == 'recording_timezone') {
                badTz <- !grepl('^UTC[+-]?[0-9:]{0,5}$', thisData[[m]])
                if(any(badTz)) {
                    warns <- addWarning(warns, deployment=thisData$deployment_code[badTz],
                                        type='Invalid Timezone',
                                        table=n,
                                        message=paste0('Timezone ', thisData[[m]][badTz], ' is invalid'))
                }
            }
            
            naVals <- is.na(thisData[[m]])
            # some columns in recordings are only mandatory if not lost
            # and not UNUSABLE
            if(n == 'recordings' &&
               m %in% onlyNotLost) {
                notLost <- !sapply(thisData$recording_device_lost, isTRUE)
                naVals <- naVals & notLost
                if('recording_quality_code' %in% names(thisData)) {
                    notUnusable <- thisData$recording_quality_code != 'UNUSABLE' | is.na(thisData$recording_quality_code)
                    naVals <- naVals & notUnusable
                }
            }
            if(n == 'detections' &&
               m == 'localization_method_code') {
                isLocalized <- !is.na(thisData$localization_latitude)
                naVals <- naVals & isLocalized
            }
            
            if(n == 'detections' &&
               m == 'localization_depth_method_code') {
                isLocalizedDepth <- !is.na(thisData$localization_depth_m)
                naVals <- naVals & isLocalizedDepth
            }
            
            if(any(naVals)) {
                warns <- addWarning(warns, deployment=unique(thisData$deployment_code[naVals]),
                                    type='NA in Mandatory Field',
                                    table=n,
                                    message=paste0("Mandatory column '",
                                                   m, "' is NA"))
            }
        }
        
        # Remove columns that werent in our loaded data and are not mandatory
        if(isTRUE(dropEmpty)) {
            keepNames <- names(thisTemp) %in% unique(c(names(thisData), thisMand))
            thisTemp <- thisTemp[keepNames]
        }
        result[[n]] <- bind_rows(thisTemp, thisData)
    }
    if(!'warnings' %in% names(result)) {
        result$warnings <- warns
    } else {
        result$warnings <- bind_rows(result$warnings, warns)
    }
    result
}

# Pretty printing helper for warnings created with `addWarning`
checkWarnings <- function(x) {
    if(!'warnings' %in% names(x) ||
       length(x$warnings) == 0) {
        return(NULL)
    }
    alls <- x$warnings$deployment == 'All'
    lapply(split(x$warnings, alls), function(y) {
        nWarns <- nrow(y)
        if(nWarns == 0) {
            return(NULL)
        }
        types <- unique(y$type)
        nDeps <- length(unique(y$deployment))
        if(y$deployment[1] == 'All') {
            warning(nWarns, ' warnings of ', length(types), ' types (', 
                    printN(types, Inf), ') affecting all deployments.')
        } else {
            warning(nWarns, ' warnings of ', length(types), ' types (',
                    printN(types, Inf), ') affecting ', nDeps, ' different deployments')
        }
    })
}
# convert date characters to proper 8601 style output
makeValidTime <- function(x) {
    if(inherits(x, 'POSIXct')) {
        return(psxTo8601(x))
    }
    out <- rep(NA_character_, length(x))
    for(i in seq_along(x)) {
        val <- x[i]
        if(is.na(val) || val == '') {
            next
        }
        datetime <- parse_date_time(
            val,
            orders=c('%Y-%m-%d %H:%M:%S',
                     '%Y/%m/%d %H:%M:%S',
                     '%Y-%m-%dT%H:%M:%SZ',
                     '%Y-%m-%dT%H:%M:%S%z'),
            truncated = 3,
            tz='UTC',
            quiet=TRUE,
            exact=TRUE)
        if(is.na(datetime)) {
            next
        }
        out[i] <- psxTo8601(datetime)
    }
    out
}

# Check if codes being used are actually in database
# Currently checks recording_device_codes, deployment device_codes, project_codes,
# site_codes, using organization_code in the check
checkDbValues <- function(x, db) {
    warns <- vector('list', length=0)
    recDevCheck <- left_join(x$recordings,
                             mutate(db$devices, JOINCHECK=TRUE),
                             by=c('organization_code', 'recording_device_codes'='device_code')
    )
    recDevCheck <- is.na(recDevCheck$JOINCHECK)
    if(any(recDevCheck)) {
        warns <- addWarning(warns, deployment=x$recordings$deployment_code[recDevCheck],
                            table='recordings',
                            type="New 'device_code'",
                            message=paste0('recording_device_code ', x$recordings$recording_device_codes[recDevCheck],
                                           ' is not present in database.devices'))
        # warning(sum(recDevCheck), ' deployments (', printN(x$recordings$deployment_code[recDevCheck], Inf),
        #         ') have recording device codes that are not present in the database')
    }
    projCheck <- left_join(x$deployments,
                           mutate(db$projects, JOINCHECK=TRUE),
                           by=c('project_code', 'organization_code'))
    missProj <- is.na(projCheck$JOINCHECK)
    if(any(missProj)) {
        warns <- addWarning(warns, deployment=x$deployments$deployment_code[missProj],
                            table='deployments',
                            type="New 'project_code'",
                            message=paste0('project_code ', x$deployments$project_code[missProj], 
                                           ' is not present in database.projects'))
        # warning(sum(missProj), ' deployments (', printN(x$deployments$deployment_code[missProj], Inf),
        #         ') have project codes that are not present in the database')
    }
    siteCheck <- left_join(x$deployments,
                           mutate(db$sites, JOINCHECK=TRUE),
                           by=c('site_code', 'organization_code'))
    missSite <- is.na(siteCheck$JOINCHECK)
    if(any(missSite)) {
        warns <- addWarning(warns, deployment=x$deployments$deployment_code[missSite],
                            table='deployments', 
                            type="New 'site_code'",
                            message=paste0('site_code ', x$deployments$site_code[missSite], 
                                           ' is not present in database.sites'))
        # warning(sum(missSite), ' deployments (', printN(x$deployments$deployment_code[missSite], Inf),
        #         ') have site codes that are not present in the database')
    }
    devCheck <- select(
        x$deployments, organization_code, deployment_code, deployment_device_codes
    ) %>% 
        mutate(deployment_device_codes = strsplit(deployment_device_codes, ',')) %>% 
        unnest(deployment_device_codes) %>% 
        left_join(
            mutate(db$devices, JOINCHECK=TRUE),
            by=c('organization_code', 'deployment_device_codes' = 'device_code')
        )
    missDev <- is.na(devCheck$JOINCHECK) & !is.na(devCheck$deployment_device_codes)
    if(any(missDev)) {
        warns <- addWarning(warns, deployment=devCheck$deployment_code[missDev],
                            table='deployments',
                            type="New 'device_code'",
                            message=paste0('device_code ', devCheck$deployment_device_codes[missDev],
                                           ' is not present in database.devices')
        )
    }
    if(!'warnings' %in% names(x)) {
        x$warnings <- warns
    } else {
        x$warnings <- bind_rows(x$warnings, warns)
    }
    x
}

# Checks if deployments, recordings, and recording_intervals
# are already in makara 
checkAlreadyDb <- function(x, db) {
    joinRequirements <- list(
        'deployments' = c('organization_code', 'deployment_code'),
        'recordings' = c('organization_code', 'deployment_code', 'recording_code'),
        'recordings_intervals' = c('deployment_code', 'recording_code', 'recording_interval_start_datetime'),
        'analyses' = c('deployment_organization_code', 'deployment_code', 'analysis_code')
    )
    for(j in names(x)) {
        if(!j %in% names(db)) {
            warning('Could not check table ', j, ', was not in DB')
            next
        }
        if(!j %in% names(joinRequirements)) {
            warning('No requirements listed for table ', j, ', did not check')
            next
        }
        if(j == 'recording_intervals') {
            db[[j]]$recording_interval_start_datetime <- format(db[[j]]$recording_interval_start_datetime,
                                                                format='%Y-%m-%d %H:%M:%S')
        }
        if(j %in% names(x)) {
            x[[j]] <- doJoinCheck(x[[j]], db[[j]], by=joinRequirements[[j]], name=j)
        }
    }
    
    x
}

# check if x is already in y by joining
doJoinCheck <- function(x, y, by, name) {
    # x <- select(x, all_of(by))
    y <- distinct(select(y, all_of(by)))
    y$JOINCHECK <- TRUE
    x <- left_join(
        x,
        y,
        by=by
    )
    newX <- is.na(x$JOINCHECK)
    x$JOINCHECK <- NULL
    x$new <- newX
    message(sum(newX), ' out of ', nrow(x), ' ', name, ' are new (not yet in Makara)')
    x
}

# Only works after `checkAlreadyDb` run to create "new" column
dropAlreadyDb <- function(x, drop=FALSE) {
    for(n in names(x)) {
        if('new' %in% names(x[[n]])) {
            if(isTRUE(drop)) {
                isNew <- x[[n]]$new
                x[[n]] <- x[[n]][isNew, ]
            }
            x[[n]]$new <- NULL
        }
    }
    x
}

# checks to drop non UTF-8 chars in character columns
fixUTF8 <- function(x) {
    x |>
        mutate(
            across(where(is.character), ~ iconv(., "UTF-8", "UTF-8", sub = ""))
        )
}

# writes template formatted CSV files to a folder - last step
writeTemplateOutput <- function(data, folder='outputs') {
    if(!dir.exists(folder)) {
        dir.create(folder)
    }
    for(n in names(data)) {
        outFile <- file.path(folder, paste0(n, '.csv'))
        if(length(data[[n]]) == 0) {
            next
        }
        data[[n]] %>% 
            fixUTF8 %>% 
            write.csv(file=outFile, row.names=FALSE, na='')
    }
}

# folder containing template .csv files
# applies column types for enforcing later
formatBasicTemplates <- function() {
    col_defs <- makaraValidatr::column_definitions
    
    result <- lapply(col_defs, function(x) {
        df <- data.frame(
            matrix(NA, nrow=1, ncol=length(x$name))
        )
        names(df) <- x$name
        df <- df[FALSE, ]
        df <- lapply(df, as.character)
        df
    })
    numCols <- lapply(col_defs, function(x) {
        x$name[x$type %in% c('float', 'integer')]
    })
    boolCols <- lapply(col_defs, function(x) {
        x$name[x$type %in% c('bool')]
    })
    for(n in names(result)) {
        for(col in numCols[[n]]) {
            result[[n]][[col]] <- as.numeric(result[[n]][[col]])
        }
        for(col in boolCols[[n]]) {
            result[[n]][[col]] <- as.logical(result[[n]][[col]])
        }
    }
    result
}

downloadBqMakara <- function(project='ggn-nmfs-pacm-dev-1', dataset='makara') {
    ds <- bq_dataset(project, dataset)
    tb_ref <- bq_dataset_query(ds, query = "select * from view_reference_codes")
    df_ref <- bq_table_download(tb_ref)
    
    tb_org <- bq_dataset_query(ds, query = "select * from view_organization_codes")
    df_org <- bq_table_download(tb_org)
    
    dep_rec_q <- bq_dataset_query(ds, query='select
                            d.organization_code,
                            d.deployment_code,
                            d.deployment_datetime,
                            d.recovery_datetime,
                            d.deployment_alias,
                            r.recording_start_datetime,
                            r.recording_end_datetime,
                            r.recording_usable_start_datetime,
                            r.recording_usable_end_datetime,
                            r.recording_code
                            from
                            deployments d
                            left join
                            recordings r
                            on d.id = r.deployment_id')
    dep_rec_df <- bq_table_download(dep_rec_q)
    list(db_ref=df_ref,
         db_org=df_org,
         db_dep_rec=dep_rec_df)
}

# transform into list of db$table_name
formatBqMakara <- function(db_raw) {
    result <- split(db_raw$db_org, db_raw$db_org$table)
    result <- lapply(result, function(x) {
        code_prefix <- switch(
            x$table[1],
            'analyses' = 'analysis_code',
            paste0(gsub('s$', '', x$table[1]), '_code')
        )
        names(x)[3] <- code_prefix
        keepCol <- which(sapply(x, function(col) !all(is.na(col))))
        x[keepCol]
    })
    result$recording_intervals <- db_raw$db_rec_int
    refs <- split(db_raw$db_ref, db_raw$db_ref$table)
    refs <- lapply(refs, function(x) {
        if(x$table[1] == 'call_types_sound_sources') {
            # split_code <- str_split(x$code, pattern=':', simplify=TRUE)
            split_code <- t(matrix(unlist(strsplit(x$code, split=':')), nrow=2))
            x$soundsource_id <- split_code[ ,2]
            x$calltype_id <- split_code[, 1]
            return(x)
        }
        code_prefix <- switch(
            x$table[1],
            'analyses' = 'analysis_code',
            paste0(gsub('s$', '', x$table[1]), '_code')
        )
        names(x)[3] <- code_prefix
        keepCol <- which(sapply(x, function(col) !all(is.na(col))))
        x[keepCol]
    })
    for(table in names(refs)) {
        result[[table]] <- refs[[table]]
    }
    result$deployments_recordings <- db_raw$db_dep_rec
    result$deployments <- left_join(
        result$deployments,
        distinct(select(result$deployments_recordings, organization_code, deployment_code, deployment_alias)),
        by=c('organization_code', 'deployment_code'),
        relationship='one-to-one'
    )
    result
}

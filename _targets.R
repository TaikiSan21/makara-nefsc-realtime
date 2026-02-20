# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
    packages = c('yaml',
                 'dplyr',
                 'tidyr',
                 'lubridate',
                 'makaraValidatr',
                 'rjson', 
                 'httr') # Packages that your targets need for their tasks.
) 
# Run the R scripts in the R/ folder with your custom functions:
# uncomment this chunk to force janky single-file update of my common functions
# updateFunctions <- download.file('https://api.github.com/repos/TaikiSan21/makaraHelpers/contents/R/makara-functions.R',
#                      destfile = 'functions/makara-functions.R',
#                      method='libcurl',
#                      headers=c('Accept'= 'application/vnd.github.v3.raw'),
#                      extra='-O -L')
tar_source('functions/makara-functions.R')
tar_source('functions/makara-realtime-functions.R')



# Replace the target list below with your own:
list(
    # params and constants ----
    tar_target(params, {
        list(
            # any deployment codes to force detection redownload
            force_download = NULL, 
            # flag to always redownload detections for active deployments
            update_active_deployment = FALSE,
            # flag to skip all work for active deployments
            skip_active_deployment = TRUE
        )
    }),
    tar_target(constants, {
        list(
            # recording_code = 'DMON2_RECORDING'
        )
    }),
    tar_target(templates, {
        # now uses makaraValidatr
        formatBasicTemplates()
    }),
    # secrets has DB passwords, smartsheets key and IDs
    tar_target(secrets_file, '.secrets/secrets.yml'),
    tar_target(secrets, {
        read_yaml(secrets_file)
    }),
    # smartsheets ----
    tar_target(rt_tracking_raw, {
        result <- readRealtimeTrackingSmart(secrets)
        result
    },
    cue=tar_cue('always')),
    tar_target(rt_tracking, {
        result <- rt_tracking_raw
        names(result) <- gsub('\\?', '', names(result))
        names(result) <- gsub('\\s?\\(makara\\)', '', names(result))
        result <- filter(
            result,
            !platform_status %in% c('Planned - Collaborator'),
            # !have_detection_data %in% c('No (no manual analyses)', 
                                        # 'No (no real-time)'),
            !is.na(organization_code)
        )
        if(isTRUE(params$skip_active_deployment)) {
            result <- filter(
                result,
                platform_status != 'Active'
            )
        }
        result
    }),
    # detections ----
    tar_target(detection_folder, 'detections'),
    tar_target(detection_downloads, {
        if(!dir.exists(detection_folder)) {
            dir.create(detection_folder)
        }
        dl_status <- select(rt_tracking, deployment_code) %>% 
            mutate(
                file = paste0(deployment_code, '_detectiondata.csv'),
                file = file.path(detection_folder, file),
                url = gsub('\\.s?html', '', rt_tracking$analysis_dataset_url),
                url = paste0(url, '_html/ptracks/manual_analysis.csv'),
                downloaded = file.exists(file)
            )
        # redownload manually labeled and currently active - comment out during dev
        # may want to adjust this to try and do an every 24h thing
        redownload <- params$force_download
        if(isTRUE(params$update_active_deployment)) {
            redownload <- unique(c(
                redownload,
                rt_tracking$deployment_code[rt_tracking$platform_status == 'Active']
            ))
        }
        if(!is.null(redownload) &&
           length(redownload) > 0) {
            dl_status$downloaded[
                dl_status$deployment_code %in% redownload
            ] <- FALSE
        }
        skipDownload <- rt_tracking$deployment_code[
            rt_tracking$have_detection_data %in% 
                c('No (no manual analyses)', 'No (no real-time)')
        ]
        dl_status$downloaded[
            dl_status$deployment_code %in% skipDownload
        ] <- TRUE
        # dl_status
        badDl <- rep(FALSE, nrow(dl_status))
        for(i in 1:nrow(dl_status)) {
            if(isTRUE(dl_status$downloaded[i])) {
                next
            }
            tryDl <- try(download.file(url=dl_status$url[i],
                                       destfile=dl_status$file[i],
                                       quiet=TRUE),
                         silent = TRUE)
            # if download fails
            if(inherits(tryDl, 'try-error') ||
               tryDl != 0) {
                badDl[i] <- TRUE
                # if partial file or something gets dl'd remove it
                if(file.exists(dl_status$file[i])) {
                    unlink(dl_status$file[i])
                }
                next
            }
            
            dl_status$downloaded[i] <- TRUE
        }
        if(any(badDl)) {
            warning(sum(badDl), ' downloads were unsuccessful (',
                    printN(dl_status$deployment_code[badDl], n=Inf),
                    ')')
        }
        dl_status
    }, cue = tar_cue('always')),
    tar_target(detection_files, {
        files <- list.files(detection_folder, full.names=TRUE)
        downloaded <- filter(detection_downloads, downloaded == TRUE)
        localNotDownload <- !files %in% downloaded$file
        if(any(localNotDownload)) {
            warning(sum(localNotDownload),
                    ' files in detection_folder are not present',
                    ' in Smartsheet metadata (',
                    printN(basename(files[localNotDownload]), Inf),
                    ')')
            files <- files[!localNotDownload]
        }
        downloadNotLocal <- !downloaded$file %in% files
        if(any(downloadNotLocal)) {
            warning(sum(downloadNotLocal), 
                    ' files do not exist and must be redownloaded (', 
                    printN(downloaded$deployment_code[downloadNotLocal], Inf),
                    ')')
        }
        files
    }, 
    cue=tar_cue('always'), format='file'
    ),
    tar_target(detections_raw, {
        lapply(detection_files, function(x) {
            result <- read.csv(x)
            result$file <- basename(x)
            result
        })
    }),
    tar_target(detections_combined, {
        # files <- list.files(detection_folder, full.names=TRUE)
        data <- lapply(detections_raw, formatRealtimeDetection)
        data <- sameNameCombiner(data)
        if(length(data) > 1) {
            warning('Column mismatch in some detection sheets')
        }
        bind_rows(data)
    }),
    # summary to use for filling NA vals later
    tar_target(detections_summary, {
        detToSummary(detections_combined, by='deployment_code')
    }),
    # deployments ----
    tar_target(deployments, {
        names <- names(templates$deployments)
        extraCols <- c('metadata_in_makara?',
                       'platform_status')
        result <- select(
            rt_tracking,
            any_of(c(names, extraCols))
        )
        result$dynamic_management_platform <- TRUE
        numCols <- c('deployment_latitude', 
                     'deployment_longitude',
                     'deployment_water_depth_m')
        for(n in numCols) {
            result[[n]] <- as.numeric(result[[n]])
        }
        if(!'deployment_comments' %in% names(result)) {
            result$deployment_comments <- NA
        }
        result <- fillNAFromOther(result,
                                  to_columns=c('deployment_longitude',
                                               'deployment_latitude'),
                                  detections_summary,
                                  from_columns=c('firstLong',
                                                 'firstLat'),
                                  by='deployment_code',
                                  comment_columns=rep('deployment_comments', 2),
                                  comment=c('Deployment location filled from detection sheet',
                                            NA),
                                  create_missing=FALSE,
                                  verbose=TRUE
        )
        result
    }),
    # recordings ----
    tar_target(recordings, {
        names <- names(templates$recordings)
        extraCols <- c('metadata_in_makara?',
                       'platform_status')
        result <- select(
            rt_tracking,
            any_of(c(names, extraCols))
        )
        numCols <- c('recording_duration_secs',
                     'recording_interval_secs',
                     'recording_sample_rate_khz',
                     'recording_bit_depth',
                     'recording_n_channels',
                     'recording_device_depth_m')
        for(n in numCols) {
            result[[n]] <- as.numeric(result[[n]])
        }
        # result$recording_code <- constants$recording_code
        result<- mutate(result,
                        recording_code = case_when(
                            grepl('DMON2', recording_device_codes) ~ 'DMON2_RECORDING',
                            grepl('DMON', recording_device_codes) ~ 'DMON_RECORDING',
                            grepl('SOUNDTRAP', recording_device_codes) ~ 'SOUNDTRAP_RECORDING',
                            .default = NA
                        ),
                        recording_device_lost = platform_status == 'Lost'
        )
        result <- fillNAFromOther(result,
                                  to_columns=c('recording_start_datetime',
                                               'recording_end_datetime'),
                                  detections_summary,
                                  from_columns=c('start',
                                                 'end'),
                                  by='deployment_code',
                                  comment_columns = rep('recording_comments', 2),
                                  comment = c("Recording start/end datetimes are taken from the start/end datetimes from the first and last periods on the real-time detection sheets because we don't currently have the raw audio for this deployment", NA),
                                  verbose=TRUE
        )
        result
    }),
    # combine and check ----
    tar_target(combined_data, {
        result <- list(deployments=deployments)
        result$recordings <- recordings
        result
    }),
    tar_target(db_check, {
        out <- combined_data
        # out <- checkAlreadyDb(out, db)
        # out <- dropAlreadyDb(out, drop=!params$export_already_in_db)
        out <- checkMakTemplate(out,
                                templates=templates,
                                ncei=FALSE,
                                dropEmpty = TRUE)
        # out <- checkDbValues(out, db)
        checkWarnings(out)
        out
    }),
    # create output ----
    tar_target(output_dir, 'outputs'),
    tar_target(output, {
        writeTemplateOutput(db_check, folder=output_dir)
        output_dir
    }, format='file'),
    tar_target(validatr, {
        validate_submission(output, 
                            output_file = file.path(output_dir, 'validation_results.csv'),
                            verbose=FALSE)
    })
)

# TODO ####

## okay for just DMON2_RECORDING recording_code? any poss of multiple recs per deploy?

# NA recording timezones
## DFO org code is new

# do we want to create tracks for gliders? idk which these are
## yes - from gcloud storage

# db existo checking


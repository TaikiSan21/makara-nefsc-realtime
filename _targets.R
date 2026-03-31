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
                 'httr',
                 'bigrquery') # Packages that your targets need for their tasks.
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


# trigger reloading database from bigquery
reload_database <- FALSE

# dont change below
if(!tar_exist_objects('db_raw')) {
    reload_database <- TRUE
}
if(isTRUE(reload_database)) {
    reload_database <- 'always'
} else if(isFALSE(reload_database)) {
    reload_database <- 'thorough'
}

# Replace the target list below with your own:
list(
    # params and constants ----
    tar_target(params, {
        list(
            new_org = list(code='DFO', name='TEMP PASS CHECK'),
            # any deployment codes to force detection redownload
            force_download = NULL, 
            # default is to not download active detections, set this TRUE to download
            download_active_detections = TRUE,
            # flag to skip all work for active deployments
            skip_active_deployment = FALSE,
            # flag to skip data already in Makara (unless needs update)
            skip_already_makara = FALSE,
            # split into 1 ana per species vs 1 ana all species
            split_analyses = FALSE,
            # skip uploading UAF_AK and OSU_AK ana/dets, metadata only
            skip_AK_analysis = TRUE,
            # fill rec start/end with analysis time for handful of deployments only
            fill_recording_times = TRUE,
            # drop_species = c()
            drop_species = c('RV-G', 'OTHE'),
            # logical flag to not write detection CSV - ignore just for testing
            skip_writing_detections = FALSE
        )
    }),
    tar_target(constants, {
        list(
            dynamic_management_platform = TRUE,
            # analysis_processing_code = 'REAL_TIME',
            # detector_codes = 'LFDCS',
            # analysis_granularity_code = 'INTERVAL',
            # analysis_quality_code = 'FULLY_VALIDATED',
            # analysis_code = 'REAL_TIME_ANALYSIS',
            # analysis_protocol_reference = 'Baumgartner & Mussoline 2011 (doi:10.1121/1.3562166); Wilder et al. 2026 (LFDCS 2.0 Reference Guide)',
            analysis_json = as.character(toJSON(
                list(MAX_MAHALANOBIS_DISTANCE = 3.0,
                     CALL_LIBRARY = 'clnb_gom7')
            )),
            track_code = 'GLIDER_TRACK',
            track_comments = 'Track generated from detection sheet coordinates'
        )
    }),
    tar_target(templates, {
        # now uses makaraValidatr
        result <- formatBasicTemplates()
        result$deployments$dynamic_management_platform <- logical(0) # temp fix for this
        result
    }),
    # secrets has DB passwords, smartsheets key and IDs
    tar_target(secrets_file, '.secrets/secrets.yml'),
    tar_target(secrets, {
        read_yaml(secrets_file)
    }),
    # db ----
    tar_target(db_raw, {
        downloadBqMakara()
    }, cue=tar_cue(reload_database)),
    tar_target(db, {
        formatBqMakara(db_raw)
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
        if(isTRUE(params$skip_already_makara)) {
            result <- filter(
                result,
                # metadata_in_makara %in% c('No', 
                #                           'Yes (update in database once retrieved)',
                #                           'No (only need recording metadata, not analyses)',
                #                           'Yes (needs updating)')
                !metadata_in_makara %in% c('Yes', 
                                           'N/A',
                                          'Yes (recording metadata only, no analyses)')
            )
        }
        result$skip_analysis <- result$have_detection_data %in% c('No (no manual analyses)', 'No (no real-time)')
        if(isTRUE(params$skip_AK_analysis)) {
            AKdeps <- grepl('^OSU_AK|^UAF_AK', result$deployment_code)
            result$skip_analysis <- result$skip_analysis | AKdeps
        }
        if(isFALSE(params$download_active_detections)) {
            activeDeps <- result$platform_status == 'Active'
            result$skip_analysis <- result$skip_analysis | activeDeps
        }
        # coalesce recording times
        if(isTRUE(params$fill_recording_times)) {
            depsToFill <- c('OSU_AK_202307_UNIT595',
                            'WHOI_SBNMS_201602_WE03',
                            'WHOI_SBNMS_201512_WE10',
                            'WHOI_SBNMS_201511_WE04',
                            'UAF_AK_201309_WE167')
            ix <- which(result$deployment_code %in% depsToFill)
            result$recording_start_datetime[ix] <- coalesce(
                result$recording_start_datetime[ix],
                result$analysis_start_datetime[ix])
            result$recording_end_datetime[ix] <- coalesce(
                result$recording_end_datetime[ix],
                result$analysis_end_datetime[ix])
        }
        result
    }),
    # analyses ----
    tar_target(analyses, {
        names <- names(templates$analyses)
        extraCols <- c('metadata_in_makara',
                       'platform_status',
                       'organization_code',
                       'pacm_permissions',
                       'skip_analysis',
                       'have_detection_data')
        result <- select(
            rt_tracking,
            any_of(c(names, extraCols)),
        ) %>% 
            filter(!skip_analysis) %>% 
            rename(
                deployment_organization_code = organization_code
            ) %>% 
            mutate(analysis_release_pacm = analysis_release_pacm == 'TRUE',
                   analysis_release_data = analysis_release_data == 'TRUE') %>%
            # filter(!have_detection_data %in% c('No (no manual analyses)', 'No (no real-time)')) %>% 
            left_join(
                select(recordings, 
                       deployment_code,
                       recording_codes=recording_code,
                       recording_sample_rate_khz),
                by='deployment_code'
            )
        if(!'analysis_sample_rate_khz' %in% names(result)) {
            result$analysis_sample_rate_khz <- NA
        }
        result$analysis_sample_rate_khz <- coalesce(
            as.numeric(result$analysis_sample_rate_khz),
            as.numeric(result$recording_sample_rate_khz) / 2
        )
        ## split ana by analysis_sound_source_codes here ----
        result$analysis_sound_source_codes <- gsub(' ', '', result$analysis_sound_source_codes)
        if(isTRUE(params$split_analyses)) {
            result <- result %>% 
                mutate(analysis_sound_source_codes = strsplit(analysis_sound_source_codes, ',')) %>% 
                unnest(analysis_sound_source_codes) %>% 
                mutate(analysis_code = paste0(analysis_sound_source_codes, '_ANALYSIS'))
            
            if(!is.null(params$drop_species) &&
               length(params$drop_species) > 0) {
                result <- filter(result, !analysis_sound_source_codes %in% params$drop_species)
            }
        }
        ##
        if(isFALSE(params$split_analyses)) {
            if(!'analysis_comments' %in% result) {
                result$analysis_comments <- NA
            }
            for(i in 1:nrow(result)) {
                splitSpec <- strsplit(result$analysis_sound_source_codes[i], ',')[[1]]
                toDrop <- splitSpec[splitSpec %in% params$drop_species]
                if(length(toDrop) > 0) {
                    sp <- paste0('"', toDrop, '"')
                    sp <- paste0(sp, collapse=',')
                    msg <- paste0('Detections for ', sp, ' were removed before Makara upload.')
                    result$analysis_comments[i] <- combineComment(result$analysis_comments[i], msg)
                    result$analysis_sound_source_codes[i] <- paste0(splitSpec[!splitSpec %in% toDrop], collapse=',')
                }
            }
            # result$analysis_code <- constants$analysis_code
        }
        result
    }),
    # detections ----
    tar_target(detection_folder, 'detections'),
    tar_target(detection_downloads, {
        if(!dir.exists(detection_folder)) {
            dir.create(detection_folder)
        }
        if(!dir.exists(file.path(detection_folder, 'active'))) {
            dir.create(file.path(detection_folder, 'active'))
        }
        dl_status <- select(rt_tracking, 
                            deployment_code,
                            have_detection_data,
                            platform_status,
                            # skip_analysis,
                            analysis_dataset_url) %>% 
            # filter(!skip_analysis) %>% 
            distinct() %>% 
            mutate(
                file = paste0(deployment_code, '_detectiondata.csv'),
                file = case_when(
                    platform_status == 'Active' ~ file.path(detection_folder, 'active', file),
                    platform_status != 'Active' ~file.path(detection_folder, file)
                ),
                url = gsub('\\.s?html', '', analysis_dataset_url),
                url = paste0(url, '_html/ptracks/manual_analysis.csv')
            )
        # remove active detection files so that they get redownloaded later
        # these should only get processed in instances where we just downloaded
        # them to prevent errors when a dep switches from active
        # active_files <- dl_status$file[dl_status$platform_status == 'Active']
        # for(f in active_files) {
        #     if(file.exists(f)) {
        #         unlink(f)
        #     }
        # }
        dl_status$downloaded <- file.exists(dl_status$file)
        # dont try to download for no detection or active deployments
        dl_status$skip <- FALSE
        dl_status$skip[
            dl_status$have_detection_data %in%  c('No (no manual analyses)', 'No (no real-time)')
        ] <- TRUE
        #  i think we should skip active detection downloads by default
        dl_status$skip[
            dl_status$platform_status == 'Active'
        ] <- TRUE
        # redownload manually labeled and currently active - comment out during dev
        # may want to adjust this to try and do an every 24h thing
        redownload <- params$force_download
        if(isTRUE(params$download_active_detections)) {
            redownload <- unique(c(
                redownload,
                dl_status$deployment_code[dl_status$platform_status == 'Active']
            ))
        }
        # overrides skip if its in force
        if(!is.null(redownload) &&
           length(redownload) > 0) {
            dl_status$downloaded[
                dl_status$deployment_code %in% redownload
            ] <- FALSE
            dl_status$skip[
                dl_status$deployment_code %in% redownload
            ] <- FALSE
        }
        
        badDl <- rep(FALSE, nrow(dl_status))
        for(i in 1:nrow(dl_status)) {
            if(isTRUE(dl_status$downloaded[i]) |
               isTRUE(dl_status$skip[i])) {
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
    tar_target(detections_raw, {
        downloaded <- filter(detection_downloads, downloaded == TRUE)
        files <- downloaded$file
        fileDNE <- !file.exists(files)
        if(any(fileDNE)) {
            warning(sum(fileDNE), ' files do not exist and must be redownloaded (',
                    printN(downloaded$deployment_code[fileDNE], Inf),
                    ')')
            files <- files[!fileDNE]
        }
        result <- lapply(files, function(x) {
            result <- read.csv(x)
            result$file <- basename(x)
            result
        })
        # remove active detection files so that they get redownloaded later
        # active_files <- downloaded$file[downloaded$platform_status == 'Active']
        # for(f in active_files) {
        #     if(file.exists(f)) {
        #         unlink(f)
        #     }
        # }
        result
    }),
    tar_target(detections_combined, {
        data <- lapply(detections_raw, baseDetectionLoader)
        data <- sameNameCombiner(data)
        if(length(data) > 1) {
            warning('Column mismatch in some detection sheets')
        }
        bind_rows(data)
    }),
    tar_target(detections, {
        result <- formatDetectionData(detections_combined)
        if(!is.null(params$drop_species) &&
           length(params$drop_species) > 0) {
            result <- filter(result, !detection_sound_source_code %in% params$drop_species)
        }
        skipDeps <- rt_tracking$deployment_code[rt_tracking$skip_analysis]
        result <- filter(result, !deployment_code %in% skipDeps)
        noMatchingAna <- !result$deployment_code %in% analyses$deployment_code
        if(any(noMatchingAna)) {
            deps <- unique(result$deployment_code[noMatchingAna])
            warning(length(deps), ' deployments in the detection data had no matching',
                    ' row in the "analyses" data, skipping (', printN(deps, 6), ')')
            result <- result[!noMatchingAna, ]
        }
        result <- left_join(
            result,
            distinct(select(analyses, 
                            deployment_organization_code, 
                            deployment_code,
                            analysis_organization_code
            )),
            by='deployment_code'
        )
        if(isTRUE(params$split_analyses)) {
            result$analysis_code <- paste0(result$detection_sound_source_code, '_ANALYSIS')
        }
        if(isFALSE(params$split_analyses)) {
            result <- left_join(
                result,
                distinct(select(analyses, deployment_code, analysis_code)),
                by='deployment_code'
            )
            # result$analysis_code <- constants$analysis_code
        }
        names <- names(templates$detections)
        result <- select(
            result,
            any_of(c(names))
        )
        result
    }),
    # summary to use for filling NA vals later
    tar_target(detections_summary, {
        detToSummary(detections_combined, by='deployment_code')
    }),
    # deployments ----
    tar_target(deployments, {
        names <- names(templates$deployments)
        extraCols <- c('metadata_in_makara',
                       'platform_status')
        result <- select(
            rt_tracking,
            any_of(c(names, extraCols))
        )
        # result$dynamic_management_platform <- TRUE
        numCols <- c('deployment_latitude', 
                     'deployment_longitude',
                     'deployment_water_depth_m')
        for(n in numCols) {
            result[[n]] <- as.numeric(result[[n]])
        }
        if(!'deployment_comments' %in% names(result)) {
            result$deployment_comments <- NA
        }
        result$deployment_device_codes <- gsub(' ', '', result$deployment_device_codes)
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
        nDeps <- table(result$deployment_code)
        if(any(nDeps > 1)) {
            multiDeps <- names(nDeps)[nDeps > 1]
            warning(length(multiDeps), ' deployments have multiple entries in Smartsheets (',
                    printN(multiDeps, Inf), ')')
        }
        # result$project_code <- formatProjectCodes(result$deployment_code)
        result
    }),
    # tracks ----
    tar_target(tracks, {
        # only make tracks if we have analysis
        # and for eletric_glider/wave_glider
        result <- select(
            analyses,
            organization_code=deployment_organization_code,
            deployment_code
        ) %>% 
            distinct() %>% 
            
            left_join(
                select(
                    deployments,
                    deployment_code,
                    deployment_platform_type_code
                ),
                by='deployment_code',
                relationship='one-to-one'
            ) %>% 
            filter(deployment_platform_type_code %in% c('ELECTRIC_GLIDER', 'WAVE_GLIDER'))
        result
    }),
    tar_target(track_positions, {
        result <- select(
            detections,
            track_position_longitude = detection_longitude,
            track_position_latitude = detection_latitude,
            track_position_datetime = detection_start_datetime,
            deployment_code) %>% 
            filter(deployment_code %in% tracks$deployment_code,
                   !is.na(track_position_latitude),
                   !is.na(track_position_longitude)) %>% 
            distinct()
        result <- result %>% 
            left_join(
                select(tracks, deployment_code, organization_code),
                by='deployment_code'
                )
        result
    }),
    # recordings ----
    tar_target(recordings, {
        names <- names(templates$recordings)
        extraCols <- c('metadata_in_makara',
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
        if(any(result$platform_status == 'Active')) {
            result$recording_comments[result$platform_status == 'Active'] <- 'Recording end time taken from detection sheet while deployment was active, time is inaccurate for final metadata'
        }
        result
    }),
    # combine and check ----
    tar_target(combined_data, {
        result <- list(deployments=deployments)
        result$recordings <- recordings
        result$analyses <- analyses
        result$detections <- detections
        result$tracks <- tracks
        result$track_positions <- track_positions
        
        ## constants ----
        # result$analyses$analysis_processing_code <- constants$analysis_processing_code
        # result$analyses$detector_codes <- constants$detector_codes
        # result$analyses$analysis_granularity_code <- constants$analysis_granularity_code
        result$analyses$analysis_json <- constants$analysis_json
        # result$analyses$analysis_quality_code <- constants$analysis_quality_code
        # result$analyses$analysis_protocol_reference <- constants$analysis_protocol_reference
        
        result$deployments$dynamic_management_platform <- constants$dynamic_management_platform
        
        result$tracks$track_code <- constants$track_code
        result$tracks$track_comments <- constants$track_comments
        
        result$track_positions$track_code <- constants$track_code
        
        result
    }),
    tar_target(db_check, {
        out <- combined_data
        out <- checkAlreadyDb(out, db)
        # out <- dropAlreadyDb(out, drop=!params$export_already_in_db)
        out <- checkMakTemplate(out,
                                templates=templates,
                                ncei=FALSE,
                                dropEmpty = TRUE)
        out <- checkDbValues(out, db)
        out <- checkDetectionData(out)
        checkWarnings(out)
        out
    }),
    # create output ----
    tar_target(output_dir, 'outputs'),
    tar_target(output, {
        to_write <- db_check
        if(isTRUE(params$skip_writing_detections)) {
            to_write$detections <- NULL
        }
        writeTemplateOutput(to_write, folder=output_dir)
        output_dir
    }, format='file'),
    tar_target(validatr, {
        # add orgs and such if we have them
        refs <- makaraValidatr::reference_tables
        refs$organizations <- bind_rows(
            refs$organizations,
            params$new_org
        )
        refs$call_types <- bind_rows(
            refs$call_types,
            params$new_call
        )
        validate_submission(output, 
                            tables=c('deployments', 
                                     # 'detections',
                                     'analyses', 
                                     'recordings',
                                     'track_positions',
                                     'tracks'
                                     ),
                            reference_tables = refs,
                            output_file = file.path(output_dir, 'validation_results.csv'),
                            verbose=FALSE)
    })
)

# TODO ####

# do we want to create tracks for gliders? idk which these are
## probably not, but they are on gcloud storage

## MULTI REC ####
# Not done yet. Analysis needs to refer to recording code(s).
# which would this be?
# Just warn for now

# may want to bump timeout >60 it triggered a couple times. At least mention
# in any tutorial doc

# current strat for actives is to put them in a separate detections/active folder
# then when it changes to not active it will no longer look for file in that path,
# so should be forced to download new one and not use older active 

# reorg params into things JW might actually change vs things
# i just set as options for development

# make trackos from det sheets easy mode

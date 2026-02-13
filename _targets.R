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
    tar_target(params, {
        list(
        )
    }),
    tar_target(constants, {
        list(
            recording_code = 'DMON2_RECORDING'
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
    tar_target(rt_tracking_raw, {
        result <- readRealtimeTrackingSmart(secrets)
        result
    }),
    tar_target(rt_tracking, {
        result <- rt_tracking_raw
        names(result) <- gsub('\\s?\\(makara\\)', '', names(result))
        result <- filter(
            result,
            !is.na(organization_code)
        )
        result
    }),
    tar_target(deployments, {
        names <- names(templates$deployments)
        extraCols <- c('metadata_in_makara?',
                       'platform_status')
        result <- select(
            rt_tracking,
            any_of(c(names, extraCols))
        )

        numCols <- c('deployment_latitude', 
                     'deployment_longitude',
                     'deployment_water_depth_m')
        for(n in numCols) {
            result[[n]] <- as.numeric(result[[n]])
        }
        result
    }),
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
        result$recording_code <- constants$recording_code
        result$recording_device_lost <- result$platform_status == 'Lost'
        result
    }),
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
                                # mandatory=mandatory_fields,
                                ncei=FALSE,
                                dropEmpty = TRUE)
        # out <- checkDbValues(out, db)
        checkWarnings(out)
        out
    }),
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

# okay for just DMON2_RECORDING recording_code? any poss of multiple recs per deploy?
# missing lat/longs okay ot pull from det forms?
# NA recording timezones
# DFO org code is new
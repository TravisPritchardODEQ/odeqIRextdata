#' PDX_BES_cont_data_pull
#'
#' This function queries continuous data from BES's Aquarius database and produces AWQMS import files. Temperature, pH, and DO
#' are queried and IR summary statistics are calculated. THe function outputs three csv files: Summary Stats,
#' continuous pH, and monitoring location information.
#'
#' @param userID UserId for Aquarius API. Contact BES
#' @param password password for Aquarius API. Contact BES
#' @param save_location filepath to save AWQMS import files
#' @export


PDX_BES_data <- function(startdate, enddate, userID, password, save_location){


# Data pull functions provided by BES -----------------------------------------------------------------------------

  process_pars <- function(...){
    pars <- list2(...)
    pars <- Filter(\(x) !is.null(x), pars)
    pars <- pars_flatten(pars)
    pars
  }

  pars_flatten <- function(x){
    lx <- sapply(x, length)
    i  <- lx > 1
    nms <- names(x)
    x[i] <- lapply(x[i], as.list)
    setNames(
      object = c(x, recursive = TRUE),
      nm     = rep(nms, lx)
    )
  }

  webportal <- function() {
    url  <- Sys.getenv("AQUARIUS_WEBPORTAL_URL")
    user <- Sys.getenv("AQUARIUS_WEBPORTAL_USER")
    pw   <- Sys.getenv("AQUARIUS_WEBPORTAL_PW")
    request(url) |>
      # req_error(
      #   is_error = \(x) resp_status(x) >= 400#,
      #   #body     = \(x) resp_body_html(x)
      # ) |>
      req_auth_basic(user, pw)
  }

  aq_webportal_req <- function(path, ...) {
    pars <- process_pars(...)
    webportal() |>
      req_url_path_append("api", "v1", path) |>
      req_url_query(!!!pars)
  }


  GetMapDataAllLocations <- function(...) {
    req <- aq_webportal_req(c("map", "locations"), ...)
    resp <- req |> req_perform()
    locs <- st_read(
      dsn = resp |> resp_body_string(),
      as_tibble = TRUE,
      quiet = TRUE
    )
  }

  GetMapDataDatasetsByParameter <- function(parameter, ...) {
    req <- aq_webportal_req(NULL, ...) |>
      req_template("map/datasets/{parameter}")
    resp <- req |> req_perform()
    locs <- st_read(
      dsn = resp |> resp_body_string(),
      as_tibble = TRUE,
      quiet = TRUE
    )
  }

  GetExportDataSet <- function(...){
    req <- aq_webportal_req(path = c("export", "data-set"), ...)
    resp <- req |> req_perform()
    ret <- resp |>
      resp_body_string() |>
      jsonlite::fromJSON()
    if(ret$numPoints[1] > 0) {
      ret <- ret$points|>
        add_column(!!!ret$dataset, .before = 1)
    }
    ret
  }

  # DEQ custom function to be able to use purrr mapping in datapull
  BES_datapull <- function(dset){
    GetExportDataSet(Dataset = dset)
  }




## API access ---------------------------------------------------------------------------------------------------


  Sys.setenv("AQUARIUS_WEBPORTAL_URL"  = "https://aquarius.portlandoregon.gov")
  Sys.setenv("AQUARIUS_WEBPORTAL_USER" = userID)
  Sys.setenv("AQUARIUS_WEBPORTAL_PW"   = password)



# Get available datasets for temp, DO, and pH ---------------------------------------------------------------------


  temp_sets <- GetMapDataDatasetsByParameter("Temperature") |>
    filter(dataSetLabel == "7DADM")
  DO_sets <- GetMapDataDatasetsByParameter("Dissolved%20oxygen")|>
    filter(dataSetLabel == "Primary")
  pH_sets <- GetMapDataDatasetsByParameter("pH")|>
    filter(dataSetLabel == "Primary")


  datasets <- c(temp_sets$dataSetIdentifier,
                DO_sets$dataSetIdentifier,
                pH_sets$dataSetIdentifier)


# Perform datapull ------------------------------------------------------------------------------------------------


  print("Starting datapull")
  BES_data <- map_dfr(datasets,  BES_datapull, .progress = TRUE)


# Data Processing -------------------------------------------------------------------------------------------------
  startdate <- lubridate::ymd(startdate)
  enddate <- lubridate::ymd(enddate)

  #filter to only high quality data
  data_fetch_hq <- BES_data %>%
    dplyr::mutate(locationIdentifier = paste0('PDX_BES-',locationIdentifier )) |>  #match existing format
    dplyr::mutate(locationIdentifier = case_when(locationIdentifier == 'PDX_BES-FC-8' ~ 'PDX_BES-FC8',
                                                 locationIdentifier == 'PDX_BES-JC1' ~ 'PDX_BES-JC-1',
                                                 locationIdentifier == 'PDX_BES-TC-4' ~ 'PDX_BES-TC4',
                                                 locationIdentifier == 'PDX_BES-TC-5' ~ 'PDX_BES-TC5',
                                                 locationIdentifier == 'PDX_BES-TC-6' ~ 'PDX_BES-TC6',
                                                 TRUE ~ locationIdentifier)) |>
    dplyr::filter(gradeCode == 100) |>
    dplyr::mutate(datetime = lubridate::ymd_hms(timestamp, tz ="America/Los_Angeles" )) |>
    dplyr::filter(datetime >= startdate-30,
                  datetime <= enddate)


  # DO formatting--------------------------------------------------------------------------------------------------------------

  if(nrow(dplyr::filter(data_fetch_hq, parameter == 'Dissolved oxygen')) > 0 ){

    data_fetch_DO <-   data_fetch_hq %>%
      dplyr::filter(parameter == 'Dissolved oxygen') %>%
      dplyr::mutate(hr =  format(datetime, "%Y-%j-%H"),
                    date = lubridate::as_date(datetime))

    #Simplify to hourly values and Stats
    hrsum <- data_fetch_DO %>%
      dplyr::group_by(locationIdentifier, hr, unit) %>%
      dplyr::summarise(date = first(date),
                       hrDTmin = min(datetime),
                       hrDTmax = max(datetime),
                       hrN = sum(!is.na(value)),
                       hrMean = mean(value, na.rm=TRUE),
                       hrMin = min(value, na.rm=TRUE),
                       hrMax = max(value, na.rm=TRUE))


    # For each date, how many hours have hrN > 0
    # remove rows with zero records in an hour.
    hrdat<- hrsum[which(hrsum$hrN >0),]

    # Summarise to daily statistics
    daydat <- hrdat %>%
      dplyr::group_by(locationIdentifier, date) %>%
      dplyr::summarise(  dDTmin = min(hrDTmin),
                         dDTmax = max(hrDTmax),
                         hrNday = length(hrN),
                         dyN = sum(hrN),
                         dyMean = mean(hrMean, na.rm=TRUE),
                         dyMin = min(hrMin, na.rm=TRUE),
                         dyMax = max(hrMax, na.rm=TRUE))

    daydat <- daydat %>%
      dplyr::rowwise() %>%
      dplyr::mutate(ResultStatusID = ifelse(hrNday >= 22, 'Final', "Rejected")) %>%
      dplyr::mutate(cmnt =ifelse(hrNday >= 22, "Generated by ORDEQ", ifelse(hrNday <= 22 & hrNday >= 20,
                                                                            paste0("Generated by ORDEQ; Estimated - ", as.character(hrNday), ' hrs with valid data in day' ),
                                                                            paste0("Generated by ORDEQ; Rejected - ", as.character(hrNday), ' hrs with valid data in day' )) ))

    daydat_station <- daydat %>%
      dplyr::filter(hrNday >= 22) %>%
      dplyr::filter(ResultStatusID != "Rejected")

    daydat_station2 <- daydat_station %>%
      dplyr::ungroup() %>%
      dplyr::group_by(locationIdentifier) %>%
      dplyr::mutate(row = dplyr::row_number(),
                    d = runner(x = data.frame(dyMax_run = dyMax, dyMean_run = dyMean, dyMin_run = dyMin,
                                              dDTmin_run = dDTmin, dDTmax_run = dDTmax),
                               k = "7 days",
                               lag = 0,
                               idx = date,
                               f = function(x) list(x)),
                    d30 =  runner(x = data.frame(dyMean_run = dyMean,  dDTmin_run = dDTmin,
                                                 dDTmax_run = dDTmax),
                                  k = "30 days",
                                  lag = 0,
                                  idx = date,
                                  f = function(x) list(x))) %>%
      dplyr::mutate(d = purrr::map(d, ~ .x %>%
                                     dplyr::summarise(ma.max7 = dplyr::case_when(length(dyMax_run) >= 6 ~  mean(dyMax_run),
                                                                                 TRUE ~ NA_real_),
                                                      ma.mean7 = dplyr::case_when(length(dyMean_run) >= 6 ~  mean(dyMean_run),
                                                                                  TRUE ~ NA_real_),
                                                      ma.min7 = dplyr::case_when(length(dyMin_run) >= 6 ~  mean(dyMin_run),
                                                                                 TRUE ~ NA_real_),
                                                      ana_startdate7 = min(dDTmin_run),
                                                      ana_enddate7   = max(dDTmax_run),
                                                      act_enddate7   = max(dDTmax_run))

      ))%>%
      dplyr::mutate(d30 = purrr::map(d30, ~ .x %>%
                                       dplyr::summarise(ma.mean30 = dplyr::case_when(length(dyMean_run) >= 29 ~  mean(dyMean_run),
                                                                                     TRUE ~ NA_real_),
                                                        ana_startdate30 = min(dDTmin_run),
                                                        ana_enddate30 =  max(dDTmax_run),
                                                        act_enddate30 = max(dDTmax_run))

      )) %>%
      tidyr::unnest_wider(d) %>%
      tidyr::unnest_wider(d30) %>%
      dplyr::mutate(ma.max7 = ifelse(row < 7, NA, ma.max7),
                    ma.mean7 = ifelse(row < 7, NA, ma.mean7),
                    ma.min7 = ifelse(row < 7, NA, ma.min7),
                    ma.mean30 = ifelse(row < 30, NA, ma.mean30)) %>%
      dplyr::select(-row)

    # Combine list to single dataframe
    sum_stats <- daydat_station2 %>%
      dplyr::arrange(locationIdentifier, date)

    #Gather summary statistics from wide format into long format
    #rename summary statistcs to match AWQMS Import COnfiguration
    sumstat_long <- sum_stats %>%
      dplyr::rename("Daily Maximum" = dyMax,
                    "Daily Minimum" = dyMin,
                    "Daily Mean"    = dyMean,
                    "7DMADMin"      = ma.min7,
                    "7DMADMean"     = ma.mean7,
                    "7DMADMax"      = ma.max7,
                    "30DMADMean"    = ma.mean30) %>%
      tidyr::gather(
        "Daily Maximum",
        "Daily Minimum",
        "Daily Mean",
        "7DMADMin",
        "7DMADMean",
        "7DMADMax",
        "30DMADMean",
        key = "StatisticalBasis",
        value = "Result",
        na.rm = TRUE
      ) %>%
      dplyr::arrange(locationIdentifier, date) |>
      ungroup()


    DO_AWQMS <- sumstat_long %>%
      dplyr::mutate(charID = 'Dissolved oxygen (DO)',
                    RsltTimeBasis = ifelse(StatisticalBasis == "7DMADMin" |
                                             StatisticalBasis == "7DMADMean" |
                                             StatisticalBasis == "7DMADMax", "7 Day",
                                           ifelse(StatisticalBasis == "30DMADMean", "30 Day", "1 Day" )),
                    ActivityType = "FMC",
                    Result.Analytical.Method.ID = "NFM 6.2.1-LUM",
                    SmplColMthd = "ContinuousPrb",
                    SmplColEquip = "Probe/Sensor",
                    SmplDepth = "",
                    SmplDepthUnit = "",
                    SmplColEquipComment = "",
                    Samplers = "",
                    Project = "Integrated Report - Call for Data",
                    AnaStartDate = "",
                    AnaStartTime = "",
                    AnaEndDate = "",
                    AnaEndTime = "",
                    ActStartDate = as.Date(format(dDTmax, "%Y-%m-%d")),
                    ActStartTime = '0:00',
                    ActEndDate = as.Date(format(dDTmax, "%Y-%m-%d")),
                    ActEndTime = '0:00',
                    RsltType = "Calculated",
                    ActStartTimeZone = "PST",
                    ActEndTimeZone = "PST",
                    AnaStartTimeZone = "",
                    AnaEndTimeZone = "",
                    Result = round(Result, digits = 2),
                    Result.Unit = "mg/l",
                    Equipment = locationIdentifier,
                    ActivityID = paste0(locationIdentifier, ":", gsub("-","",ActStartDate), ":", ActivityType),
                    ResultStatusID = as.character(ResultStatusID),
                    Monitorining.Location.ID = locationIdentifier
      ) %>%
      dplyr::select(charID,
                    Result,
                    Result.Unit,
                    Result.Analytical.Method.ID,
                    RsltType,
                    ResultStatusID,
                    StatisticalBasis,
                    RsltTimeBasis,
                    cmnt,
                    ActivityType,
                    Monitorining.Location.ID,
                    SmplColMthd,
                    SmplColEquip,
                    SmplDepth,
                    SmplDepthUnit,
                    SmplColEquipComment,
                    Samplers,
                    Equipment,
                    Project,
                    ActStartDate,
                    ActStartTime,
                    ActStartTimeZone,
                    ActEndDate,
                    ActEndTime,
                    ActEndTimeZone,
                    AnaStartDate,
                    AnaStartTime,
                    AnaStartTimeZone,
                    AnaEndDate,
                    AnaEndTime,
                    AnaEndTimeZone,
                    ActivityID)
  }




  # Temperature formatting -----------------------------------------------------------------------------------------------------
  if(nrow(dplyr::filter(data_fetch_hq, parameter == 'Temperature')) > 0 ){


    data_fetch_temp <- data_fetch_hq %>%
      dplyr::filter(parameter == 'Temperature') %>%
      dplyr::transmute(charID = "Temperature, water",
                       Result = value,
                       Result.Unit = unit,
                       Result.Analytical.Method.ID = "THM01",
                       RsltType = "Calculated",
                       ResultStatusID = as.character(approvalLevel),
                       StatisticalBasis = '7DMADMax',
                       RsltTimeBasis = ifelse(StatisticalBasis == "7DMADMax", "7 day", "1 Day" ),
                       cmnt = comment,
                       ActivityType = "FMC",
                       Monitorining.Location.ID = locationIdentifier,
                       SmplColMthd = "ContinuousPrb",
                       SmplColEquip = "Probe/Sensor",
                       SmplDepth = "",
                       SmplDepthUnit = "",
                       SmplColEquipComment = "",
                       Samplers = "",
                       Equipment = "Continuous Probe",
                       Project = "Integrated Report - Call for Data",
                       ActStartDate = datetime,
                       ActStartTime = "0:00",
                       ActStartTimeZone = "PST",
                       ActEndDate = datetime,
                       ActEndTime = "0:00",
                       ActEndTimeZone = "PST",
                       AnaStartDate = "",
                       AnaStartTime = "",
                       AnaStartTimeZone = "",
                       AnaEndDate = "",
                       AnaEndTime = "",
                       AnaEndTimeZone = "",
                       ActivityID = paste0(Monitorining.Location.ID, ":", gsub("-","",ActStartDate), ":", ActivityType)
      )



  }

  # pH formatting--------------------------------------------------------------------------------------------------------------

  if(nrow(dplyr::filter(data_fetch_hq, parameter == 'pH')) > 0 ){

    data_fetch_pH <- data_fetch_hq %>%
      dplyr::filter(parameter == 'pH') |>
      dplyr::transmute('Monitoring_Location_ID' = locationIdentifier,
                       "Activity_start_date" = format(datetime, "%Y/%m/%d"),
                       'Activity_Start_Time' =format(datetime, "%H:%M:%S"),
                       'Activity_Time_Zone' = "",
                       'Equipment_ID' = locationIdentifier,
                       'Characteristic_Name' = 'pH',
                       "Result_Value" = value,
                       "Result_Unit" = unit,
                       "Result_Status_ID" = gradeCode)




    pH_deployments <-   data_fetch_pH %>%
      dplyr::mutate(datetime = lubridate::ymd_hms(paste(Activity_start_date,Activity_Start_Time ))) %>%
      dplyr::group_by(Monitoring_Location_ID, Equipment_ID) %>%
      dplyr::summarise(Activity_start_date_time = min(datetime),
                       Activity_end_date_time  = max(datetime + lubridate::seconds(1)),
                       Activity_start_end_time_Zone = dplyr::first(Activity_Time_Zone)) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(Media = "Water",
                    Media_subdivision = "Surface Water",
                    Project_ID = "Integrated Report - Call for Data",
                    Alternate_Project_ID = "",
                    Alternate_Project_ID2 = "",
                    Frequency_on_minutes = "",
                    Depth_in_m = "")
  }


# Put it all together ---------------------------------------------------------------------------------------------

  sum_stats <-  bind_rows(data_fetch_temp, DO_AWQMS)

  write.csv(sum_stats, file = paste0(save_location,"PDX_BES_sum_stats.csv"), row.names = FALSE)
  write.csv(data_fetch_pH, file = paste0(save_location,"PDX_BES_pH_raw.csv"), row.names = FALSE)
  write.csv(pH_deployments, file = paste0(save_location,"PDX_BES_pH_deployments.csv"), row.names = FALSE)




  BES_data <- list(sum_stats = sum_stats,
                   pH_raw = data_fetch_pH,
                   pH_deployments = pH_deployments)

  return(BES_data)



}



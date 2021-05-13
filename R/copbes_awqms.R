
#' Retrieve surface water monitoring results from The City of Portland Bureau of Environmental Services and creates and
#' saves AWQMS import files
#'
#' Data is retrieved from City of Portland Bureau of Environmental Services database.
#' https://aquarius.portlandoregon.gov/ using the copbes_data function
#'
#' @param data_inventory Required datafarme of available data. Columns should match augments of the copbes_data function:
#'   * 'station'
#'   * 'startdate'
#'   * 'enddate'
#'   * 'char'
#' @param project project name for AWQMS
#' @export
#' @return list of deployments, sumstats, and raw pH continuous data



copbes_AWQMS <- function(data_inventory,
                         project) {

# Testing ---------------------------------------------------------------------------------------------------------




#
# BES_inventory_import <- read.csv("C:/Users/tpritch/Documents/odeqIRextdata/PortlandBes_data_inventory.csv")
#
#
# BES_inventory <- BES_inventory_import %>%
#   transmute(station = LocationIdentifier,
#             startdate = '2016-01-01',
#             enddate = '2020-12-31',
#             char = gsub("@.*$","",Identifier)) %>%
#   filter(char %in% c('Dissolved oxygen.Primary',
#                      'pH.Primary',
#                      'Specific conductance.Primary',
#                      'Temperature.Primary', 'Temperature.7DADM'))
#
# data_inventory <- BES_inventory
#   #
# project = "call for data 2022"


# Error checking ---------------------------------------------------------------------------------------------------------


  if(missing(project)){
    stop("'project' cannot be null.")

  }






# fetch data ------------------------------------------------------------------------------------------------------




data_fetch <- purrr::pmap_dfr(data_inventory, copbes_data)

#Filter out lower quality data
 # grade codes
   # -3  = Gap
   # -2  = UNUSABLE
   # -1  = UNSP
   # 0   = UNDEF
   # 20  = Poor
   # 50  = Suspect
   # 100 = Good

data_fetch_hq <- data_fetch %>%
  dplyr::filter(!is.na(Result.Value)) %>%
  dplyr::filter(Grade.Code == 100)

if(nrow(data_fetch_hq) < 1){

  deployments <- NULL
  AWQMS_sum_stats <- NULL
  data_fetch_pH <-  NULL

  con_data_list <-list(  deployments=as.data.frame(deployments),
                         sumstats=as.data.frame(AWQMS_sum_stats),
                         pH_continuous =as.data.frame(data_fetch_pH))

  return(con_data_list)

} else {


# Deployments -------------------------------------------------------------

  BES_mloc_lu_select <- BES_mloc_lu %>%
    select(MLocID, LocationIdentifier)

  data_fetch_hq <- data_fetch_hq %>%
    left_join(BES_mloc_lu_select, by = c('Monitoring_Location_ID' = "LocationIdentifier")) %>%
    mutate(Monitoring_Location_ID = MLocID) %>%
    select(-MLocID)


deployments <-   data_fetch_hq %>%
  dplyr::group_by(Monitoring_Location_ID) %>%
  dplyr::summarise(Activity_start_date_time = min(datetime),
                   Activity_end_date_time  = max(datetime + lubridate::seconds(1)),
                   Activity_start_end_time_Zone = lubridate::tz(datetime)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(Equipment_ID = paste0(Monitoring_Location_ID, "-", format(Activity_start_date_time, "%Y%m"), "-", format(Activity_end_date_time, "%Y%m")),
                Media = "Water",
                Media_subdivision = "Surface Water",
                Project_ID = project,
                Alternate_Project_ID = "",
                Alternate_Project_ID2 = "",
                Frequency_on_minutes = "",
                Depth_in_m = "")

# openxlsx::write.xlsx(deployments, file = paste0(save_location, "cont-", station, "-deployments.xlsx" ))

data_fetch_hq <- data_fetch_hq %>%
  dplyr::left_join(select(deployments, Monitoring_Location_ID, Equipment_ID))
# Temperature -----------------------------------------------------------------------------------------------------



if(nrow(dplyr::filter(data_fetch_hq, Characteristic.Name == 'Temperature.7DADM')) > 0 ){


data_fetch_temp <- data_fetch_hq %>%
  dplyr::filter(Characteristic.Name == 'Temperature.7DADM') %>%
  dplyr::transmute(charID = "Temperature, water",
                   Result = Result.Value,
                   Result.Unit = Result.Unit,
                   Result.Analytical.Method.ID = "T-170.1",
                   RsltType = "Calculated",
                   ResultStatusID = as.character(Approval.Level),
                   StatisticalBasis = '7DMADMax',
                   RsltTimeBasis = ifelse(StatisticalBasis == "7DMADMax", "7 day", "1 Day" ),
                   cmnt = Comment,
                   ActivityType = "FMC",
                   Monitoring_Location_ID = Monitoring_Location_ID,
                   SmplColMthd = "ContinuousPrb",
                   SmplColEquip = "Probe/Sensor",
                   SmplDepth = "",
                   SmplDepthUnit = "",
                   SmplColEquipComment = "",
                   Samplers = "",
                   Equipment = "Continuous Probe",
                   Project = project,
                   ActStartDate = as.character(datetime),
                   ActStartTime = "0:00",
                   ActStartTimeZone = lubridate::tz(datetime),
                   ActEndDate = as.character(datetime),
                   ActEndTime = "0:00",
                   ActEndTimeZone = lubridate::tz(datetime),
                   AnaStartDate = as.character(datetime - days(6)),
                   AnaStartTime = "0:00",
                   AnaStartTimeZone = lubridate::tz(datetime),
                   AnaEndDate = as.character(datetime),
                   AnaEndTime = "0:00",
                   AnaEndTimeZone = lubridate::tz(datetime),
                   ActivityID = NA
  )



}

# raw Temp --------------------------------------------------------------------------------------------------------
#If no 7DADM value exists, calculate

#Stations with no 7DADM values
temp_raw_stations <- data_inventory %>%
  group_by(station) %>%
  summarise(temp_in_group =any(grepl("Temperature", char)),
            dadm7_in_group = any(stringr::str_detect(char, 'Temperature.7DADM'))) %>%
  filter(temp_in_group == TRUE & dadm7_in_group == FALSE) %>%
  pull(station)





if(nrow(dplyr::filter(data_fetch_hq, Monitoring_Location_ID %in% temp_raw_stations)) > 0){

  data_fetch_raw_temp <-   data_fetch_hq %>%
    dplyr::filter( Monitoring_Location_ID %in% temp_raw_stations,
                   Characteristic.Name == 'Temperature.Primary') %>%
    dplyr::mutate(hr =  format(datetime, "%Y-%j-%H"))

  #Simplify to hourly values and Stats
  hrsum <- data_fetch_raw_temp %>%
    dplyr::group_by(Monitoring_Location_ID, Equipment_ID, hr, Result.Unit) %>%
    dplyr::summarise(date = as.Date(dplyr::first(datetime)),
                     hrDTmin = min(datetime),
                     hrDTmax = max(datetime),
                     hrN = sum(!is.na(Result.Value)),
                     hrMean = mean(Result.Value, na.rm=TRUE),
                     hrMin = min(Result.Value, na.rm=TRUE),
                     hrMax = max(Result.Value, na.rm=TRUE))



  # For each date, how many hours have hrN > 0
  # remove rows with zero records in an hour.
  hrdat<- hrsum[which(hrsum$hrN >0),]

  # Summarise to daily statistics
  daydat <- hrdat %>%
    dplyr::group_by(Monitoring_Location_ID, Equipment_ID, date) %>%
    dplyr::summarise(dDTmin = min(hrDTmin),
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


  #Filter dataset to only look at 1 monitoring location at a time
  daydat_station <- daydat %>%
    dplyr::filter(hrNday >= 22) %>%
    dplyr::filter(ResultStatusID != "Rejected")


  daydat_station2 <- daydat_station %>%
    dplyr::ungroup() %>%
    dplyr::group_by(Equipment_ID) %>%
    dplyr::mutate(row = dplyr::row_number(),
                  d = runner(x = data.frame(dyMax_run = dyMax, dDTmin_run = dDTmin,
                                            dDTmax_run = dDTmax),
                             k = "7 days",
                             lag = 0,
                             idx = date,
                             f = function(x) list(x))) %>%
    dplyr::mutate(d = purrr::map(d, ~ .x %>%
                                   dplyr::summarise(ma.max7 = dplyr::case_when(length(dyMax_run) >= 6 ~  mean(dyMax_run),
                                                                               TRUE ~ NA_real_),
                                                    ana_startdate7 = min(dDTmin_run),
                                                    ana_enddate7   = max(dDTmax_run),
                                                    act_enddate7   = max(dDTmax_run))

    ))%>%
    tidyr::unnest_wider(d) %>%
    dplyr::mutate(ma.max7 = ifelse(row < 7, NA, ma.max7)) %>%
    dplyr::select(-row)



  # Combine list to single dataframe
  sum_stats_temp <- daydat_station2 %>%
    dplyr::arrange(Monitoring_Location_ID, Equipment_ID, date)

  #add any missing columns
  sumstat_long_cols <- c("ana_startdate7", "ana_startdate30", "ana_enddate7", "ana_enddate30",
                         "ma.max7", 'ma.min7', 'ma.mean7', 'ma.mean30')

  missing_cols <- setdiff(sumstat_long_cols, names(sum_stats_temp))

  sum_stats_temp[missing_cols] <- NA


  #Gather summary statistics from wide format into long format
  #rename summary statistcs to match AWQMS Import COnfiguration
  sumstat_long <- sum_stats_temp %>%
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
    dplyr::arrange(Monitoring_Location_ID, date)

  #   left_join(Audits_unique, by = c("Monitoring.Location.ID", "charID" = "Characteristic.Name") )
  Temp_AWQMS <- sumstat_long %>%
    ungroup() %>%
    dplyr::mutate(charID = "Temperature, water",
                  RsltTimeBasis = ifelse(StatisticalBasis == "7DMADMin" |
                                           StatisticalBasis == "7DMADMean" |
                                           StatisticalBasis == "7DMADMax", "7 Day",
                                         ifelse(StatisticalBasis == "30DMADMean", "30 Day", "1 Day" )),
                  ActivityType = "FMC",
                  Result.Analytical.Method.ID = "T-170.1",
                  SmplColMthd = "ContinuousPrb",
                  SmplColEquip = "Probe/Sensor",
                  SmplDepth = "",
                  SmplDepthUnit = "",
                  SmplColEquipComment = "",
                  Samplers = "",
                  Project = project,
                  AnaStartDate = case_when(RsltTimeBasis == "1 Day" ~ format(dDTmin, format="%Y-%m-%d"),
                                           RsltTimeBasis == "7 Day" ~ format(ana_startdate7, format="%Y-%m-%d"),
                                           RsltTimeBasis == "30 Day" ~ format(ana_startdate30, format="%Y-%m-%d")),
                  AnaStartTime = case_when(RsltTimeBasis == "1 Day" ~ format(dDTmin, format="%H:%M:%S"),
                                           RsltTimeBasis == "7 Day" ~ format(ana_startdate7, format="%H:%M:%S"),
                                           RsltTimeBasis == "30 Day" ~ format(ana_startdate30, format="%H:%M:%S")),
                  AnaEndDate = case_when(RsltTimeBasis == "1 Day" ~ format(dDTmax, format="%Y-%m-%d"),
                                         RsltTimeBasis == "7 Day" ~ format(ana_enddate7, format="%Y-%m-%d"),
                                         RsltTimeBasis == "30 Day" ~ format(ana_enddate30, format="%Y-%m-%d")),
                  AnaEndTime = case_when(RsltTimeBasis == "1 Day" ~ format(dDTmax, format="%H:%M:%S"),
                                         RsltTimeBasis == "7 Day" ~ format(ana_enddate7, format="%H:%M:%S"),
                                         RsltTimeBasis == "30 Day" ~ format(ana_enddate30, format="%H:%M:%S")),
                  ActStartDate = format(date, format="%Y-%m-%d"),
                  ActStartTime = "0:00",
                  ActEndDate = AnaEndDate,
                  ActEndTime = AnaEndTime,
                  RsltType = "Calculated",
                  ActStartTimeZone = lubridate::tz(date),
                  ActEndTimeZone = lubridate::tz(date),
                  AnaStartTimeZone = lubridate::tz(date),
                  AnaEndTimeZone = lubridate::tz(date),
                  Result = round(Result, digits = 2),
                  Result.Unit = "deg C",
                  Equipment = "Continuous Probe",
                  ActivityID = NA,
                  ResultStatusID = as.character(ResultStatusID)
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
                  Monitoring_Location_ID,
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

# DO --------------------------------------------------------------------------------------------------------------

if(nrow(dplyr::filter(data_fetch_hq, Characteristic.Name == 'Dissolved oxygen.Primary')) > 0 ){

data_fetch_DO <-   data_fetch_hq %>%
  dplyr::filter(Characteristic.Name == 'Dissolved oxygen.Primary') %>%
  dplyr::mutate(hr =  format(datetime, "%Y-%j-%H"))

#Simplify to hourly values and Stats
hrsum <- data_fetch_DO %>%
  dplyr::group_by(Monitoring_Location_ID, Equipment_ID, hr, Result.Unit) %>%
  dplyr::summarise(date = as.Date(dplyr::first(datetime)),
                   hrDTmin = min(datetime),
                   hrDTmax = max(datetime),
                   hrN = sum(!is.na(Result.Value)),
                   hrMean = mean(Result.Value, na.rm=TRUE),
                   hrMin = min(Result.Value, na.rm=TRUE),
                   hrMax = max(Result.Value, na.rm=TRUE))



# For each date, how many hours have hrN > 0
# remove rows with zero records in an hour.
hrdat<- hrsum[which(hrsum$hrN >0),]

# Summarise to daily statistics
daydat <- hrdat %>%
  dplyr::group_by(Monitoring_Location_ID, Equipment_ID, date) %>%
  dplyr::summarise(dDTmin = min(hrDTmin),
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


#Filter dataset to only look at 1 monitoring location at a time
daydat_station <- daydat %>%
  dplyr::filter(hrNday >= 22) %>%
  dplyr::filter(ResultStatusID != "Rejected")


daydat_station2 <- daydat_station %>%
  dplyr::ungroup() %>%
  dplyr::group_by(Equipment_ID) %>%
  dplyr::mutate(row = dplyr::row_number(),
                d = runner(x = data.frame(dyMean_run = dyMean, dyMin_run = dyMin, dDTmin_run = dDTmin,
                                          dDTmax_run = dDTmax),
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
                                 dplyr::summarise(ma.mean7 = dplyr::case_when(length(dyMean_run) >= 6 ~  mean(dyMean_run),
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
  dplyr::mutate(ma.mean7 = ifelse(row < 7, NA, ma.mean7),
                ma.min7 = ifelse(row < 7, NA, ma.min7),
                ma.mean30 = ifelse(row < 30, NA, ma.mean30)) %>%
  dplyr::select(-row)



# Combine list to single dataframe
sum_stats_DO <- daydat_station2 %>%
  dplyr::arrange(Monitoring_Location_ID, Equipment_ID, date)

#add any missing columns
sumstat_long_cols <- c("ana_startdate7", "ana_startdate30", "ana_enddate7", "ana_enddate30",
                          "ma.max7", 'ma.min7', 'ma.mean7', 'ma.mean30')

missing_cols <- setdiff(sumstat_long_cols, names(sum_stats_DO))

sum_stats_DO[missing_cols] <- NA


#Gather summary statistics from wide format into long format
#rename summary statistcs to match AWQMS Import COnfiguration
sumstat_long <- sum_stats_DO %>%
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
  dplyr::arrange(Monitoring_Location_ID, date)

#   left_join(Audits_unique, by = c("Monitoring.Location.ID", "charID" = "Characteristic.Name") )
DO_AWQMS <- sumstat_long %>%
  ungroup() %>%
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
         Project = project,
         AnaStartDate = case_when(RsltTimeBasis == "1 Day" ~ format(dDTmin, format="%Y-%m-%d"),
                                  RsltTimeBasis == "7 Day" ~ format(ana_startdate7, format="%Y-%m-%d"),
                                  RsltTimeBasis == "30 Day" ~ format(ana_startdate30, format="%Y-%m-%d")),
         AnaStartTime = case_when(RsltTimeBasis == "1 Day" ~ format(dDTmin, format="%H:%M:%S"),
                                  RsltTimeBasis == "7 Day" ~ format(ana_startdate7, format="%H:%M:%S"),
                                  RsltTimeBasis == "30 Day" ~ format(ana_startdate30, format="%H:%M:%S")),
         AnaEndDate = case_when(RsltTimeBasis == "1 Day" ~ format(dDTmax, format="%Y-%m-%d"),
                                RsltTimeBasis == "7 Day" ~ format(ana_enddate7, format="%Y-%m-%d"),
                                RsltTimeBasis == "30 Day" ~ format(ana_enddate30, format="%Y-%m-%d")),
         AnaEndTime = case_when(RsltTimeBasis == "1 Day" ~ format(dDTmax, format="%H:%M:%S"),
                                RsltTimeBasis == "7 Day" ~ format(ana_enddate7, format="%H:%M:%S"),
                                RsltTimeBasis == "30 Day" ~ format(ana_enddate30, format="%H:%M:%S")),
         ActStartDate = format(date, format="%Y-%m-%d"),
         ActStartTime = "0:00",
         ActEndDate = AnaEndDate,
         ActEndTime = AnaEndTime,
         RsltType = "Calculated",
         ActStartTimeZone = lubridate::tz(date),
         ActEndTimeZone = lubridate::tz(date),
         AnaStartTimeZone = lubridate::tz(date),
         AnaEndTimeZone = lubridate::tz(date),
         Result = round(Result, digits = 2),
         Result.Unit = "mg/l",
         Equipment = "Continuous Probe",
         ActivityID = NA,
         ResultStatusID = as.character(ResultStatusID)
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
         Monitoring_Location_ID,
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





#Write summary stat tables. CHeck to see if parameter exists first.


if(!exists("DO_AWQMS")) {
  DO_AWQMS <- NULL

}

if(!exists("data_fetch_temp")) {
  data_fetch_temp <- NULL

}

if( !exists("Temp_AWQMS")){
  Temp_AWQMS <- NULL

}



AWQMS_sum_stats <- dplyr::bind_rows(DO_AWQMS, data_fetch_temp, Temp_AWQMS) %>%
  dplyr::arrange(Monitoring_Location_ID, ActStartDate)



# pH --------------------------------------------------------------------------------------------------------------

if(nrow(dplyr::filter(data_fetch_hq, Characteristic.Name == 'pH.Primary')) > 0 ){

data_fetch_pH <- data_fetch_hq %>%
  dplyr::filter(Characteristic.Name == 'pH.Primary') %>%
  dplyr::transmute('Monitoring_Location_ID' = Monitoring_Location_ID,
                   "Activity_start_date" = format(datetime, "%Y/%m/%d"),
                   'Activity_Start_Time' =format(datetime, "%H:%M:%S"),
                   'Activity_Time_Zone' = "",
                   'Equipment_ID' = Equipment_ID,
                   'Characteristic_Name' = 'pH',
                   "Result_Value" = Result.Value,
                   "Result_Unit" = Result.Unit,
                   "Result_Status_ID" = Approval.Level)


pH_deployments <- data_fetch_hq %>%
  dplyr::filter(Characteristic.Name == 'pH.Primary') %>%
  dplyr::group_by(Monitoring_Location_ID, Equipment_ID) %>%
  dplyr::summarise(Activity_start_date_time = min(datetime),
                   Activity_end_date_time  = max(datetime + lubridate::seconds(1)),
                   Activity_start_end_time_Zone = lubridate::tz(datetime)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(Media = "Water",
                Media_subdivision = "Surface Water",
                Project_ID = project,
                Alternate_Project_ID = "",
                Alternate_Project_ID2 = "",
                Frequency_on_minutes = "",
                Depth_in_m = "")





#openxlsx::write.xlsx(data_fetch_pH, file = paste0(save_location, "cont_pH-", station, ".xlsx" ))

} else {
  data_fetch_pH <- NULL


}

con_data_list <-list(  deployments=as.data.frame(deployments),
                       sumstats=as.data.frame(AWQMS_sum_stats),
                       pH_continuous =as.data.frame(data_fetch_pH),
                       pH_deployments = as.data.frame(pH_deployments))

return(con_data_list)



}

}

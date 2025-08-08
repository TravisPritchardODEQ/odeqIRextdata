#' NWIS_cont_data_pull
#'
#' This function queries continuous data from the NWIS database and produces AWQMS import files. Temperature, pH, and DO
#' are queried and IR summary statistics are calculated. THe function outputs three csv files: NWIS Sumamry Stats,
#' NWIS continuous pH, and NWIS monitoring location information. The function has no outputs, the CSV files serve as
#' outputs.
#'
#' @param start.date Start date of data pull. Format: "yyyy-mm-dd"
#' @param end.date End date of datapull. Format: "yyyy-mm-dd"
#' @param project Name of project for AWQMS
#' @param save_location Folder to save output files to. Should end with a /
#' @param stateCD Statecode for datapull. Defaults to 'or'
#' @export
#' @examples
#'\dontrun{
#'NWIS_cont_data_pull(start.date = '2021-01-01', end.date = '2021-12-31', save_location = "E:/Documents/",
#'                    project = "Example Project")
#'}
#'


NWIS_cont_data_pull <- function(start.date, end.date, save_location, project, stateCD = "or",
                                split_file = TRUE, check_dups = FALSE){




# Testing ---------------------------------------------------------------------------------------------------------
#
# start.date <- "2016-01-01"
# end.date <- "2016-12-31"
# save_location <- 'C:/Users/tpritch/Documents/Test CFD files/'
# project = "2022 IR Call for Data"
# stateCD = "or"
# split_file = TRUE
# #



  options(scipen=999)


  print("get oregon sites")
  sites_import <- dataRetrieval::whatNWISsites(stateCD = stateCD,
                         hasDataTypeCd=c("dv","uv"))

  # remove wells and outfalls
sites <- dplyr::filter(sites_import, !site_tp_cd %in% c('GW', 'FA-OF'))




  USGS_stations <- sites$site_no
  statcds = c("00001", "00002", "00003", "00008")
  # 00001 = max, 00002 = min, 00003 = mean, 00008 = median
  pCodes = c("00010", "00400", "00300", "00301")
  # 00010 = temp, 00400 = ph, 00300 = DO mg/L, 00301 = DO sat

  # this will get you all data. Way too big for 10 years data
  # my computer runs out of resources to convert from wide to long
  # need to run each param seperate
  #
  # nwis.sum.stats.temp <- readNWISdv(siteNumbers = USGS_stations,
  #                              parameterCd = pCodes,
  #                              startDate = start.date,
  #                              endDate = end.date,
  #                              statCd = statcds)

  # nwis.sum.stats <- renameNWISColumns(nwis.sum.stats)


  # this will get you all data. Way too big for 10 years data
  # my computer runs out of resources to convert from wide to long
  # need to run each param seperate
  #
  # nwis.sum.stats.temp <- readNWISdv(siteNumbers = USGS_stations,
  #                              parameterCd = pCodes,
  #                              startDate = start.date,
  #                              endDate = end.date,
  #                              statCd = statcds)

  # nwis.sum.stats <- renameNWISColumns(nwis.sum.stats)


  # NWIS temperature --------------------------------------------------------



print("Query NWIS Temperature begin....")
    nwis.sum.stats.temp <- dataRetrieval::readNWISdv(siteNumbers = USGS_stations,
                                    parameterCd = "00010",
                                    startDate = start.date,
                                    endDate = end.date,
                                    statCd = statcds)

    print("Query NWIS Temperature end")

  nwis.sum.stats.temp <- dataRetrieval::renameNWISColumns(nwis.sum.stats.temp)



  # Calculate 7 day moving average
  # lag uses 6 because the 7 day moving average is inclusive of the date
  # If 1 day is missing from period, do not caluclate average

  temp4ma <- nwis.sum.stats.temp %>%
    group_by(site_no) %>%
    dplyr::mutate(row = dplyr::row_number(),
                  d = runner(x = data.frame(dyMax_run = Wtemp_Max,  datetime = Date),
                             k = "7 days",
                             lag = 0,
                             idx = Date,
                             f = function(x) list(x))) %>%
    dplyr::mutate(d = purrr::map(d, ~ .x %>%
                                   dplyr::summarise(ma.max7 = dplyr::case_when(length(dyMax_run) >= 6 ~  mean(dyMax_run),
                                                                               TRUE ~ NA_real_
                                   ),
                                   ana_startdate7 = min(datetime),
                                   ana_enddate7 =  max(datetime),
                                   act_enddate7 = max(datetime))
    ))%>%
    tidyr::unnest_wider(d) %>%
    dplyr::mutate(ma.max7 = ifelse(row < 7, NA, ma.max7)) %>%
    dplyr::select(-row)




  nwis.sum.stats.temp.gather <- temp4ma %>%
    tidyr::gather(Wtemp_Max,
           Wtemp,
           Wtemp_Min,
           ma.max7, key = "stat", value = "result", na.rm = TRUE) %>%
    dplyr::mutate(qual = ifelse(stat == "Wtemp_Max" | stat == "ma.max7", Wtemp_Max_cd,
                         ifelse(stat == "Wtemp", Wtemp_cd,
                                ifelse(stat == "Wtemp_Min", Wtemp_Min_cd, "ERROR" )))) %>%
    dplyr::select(agency_cd, site_no, Date, stat, result, qual, ana_startdate7,ana_enddate7,act_enddate7  ) %>%
    dplyr::arrange(site_no, Date)




  nwis.sum.stats.temp.AWQMS <- nwis.sum.stats.temp.gather %>%
    dplyr::ungroup() %>%
    dplyr::transmute(CharID = "Temperature, water",
              Result =  round(result, 1),
              Unit = "deg C",
              Method = "THM01",
              RsltType = "Calculated",
              Qualcd = qual,
              StatisticalBasis = dplyr::case_when(stat == "ma.max7" ~"7DMADMax",
                                                  stat == "Wtemp_Max" ~ "Daily Maximum",
                                                  stat == "Wtemp" ~ "Daily Mean",
                                                  stat == "Wtemp_Min" ~ "Daily Minimum",
                                                  TRUE ~  "ERROR"),
              RsltTimeBasis = ifelse(StatisticalBasis == "7DMADMax", "7 Day", "1 Day" ),
              DEQ_RsltComment = ifelse(StatisticalBasis == "7DMADMax", "Generated by ORDEQ", "" ),
              ActivityType = "FMC",
              SiteID = site_no,
              SmplColMthd = "ContinuousPrb",
              SmplColEquip = "Probe/Sensor",
              SmplDepth = "",
              SmplDepthUnit = "",
              SmplColEquipComment = "",
              Samplers = "",
              SmplEquipID = "Continuous Probe",
              Project = project,
              ActStartDate = Date,
              ActStartTime = "0:00",
              ActStartTimeZone = "UTC",
              ActEndDate =Date,
              ActEndTime = "0:00",
              ActEndTimeZone = "UTC",
              AnaStartDate = case_when(RsltTimeBasis == "1 Day" ~ format(Date, format="%Y-%m-%d"),
                                       RsltTimeBasis == "7 Day" ~ format(ana_startdate7, format="%Y-%m-%d")),
              AnaStartTime = case_when(RsltTimeBasis == "1 Day" ~ format(Date, format="%H:%M:%S"),
                                       RsltTimeBasis == "7 Day" ~ format(ana_startdate7, format="%H:%M:%S")),
              AnaStartTimeZone = "UTC",
              AnaEndDate = case_when(RsltTimeBasis == "1 Day" ~ format(Date, format="%Y-%m-%d"),
                                     RsltTimeBasis == "7 Day" ~ format(ana_enddate7, format="%Y-%m-%d")),
              AnaEndTime = case_when(RsltTimeBasis == "1 Day" ~ format(Date, format="%H:%M:%S"),
                                     RsltTimeBasis == "7 Day" ~ format(ana_enddate7, format="%H:%M:%S")),
              AnaEndTimeZone = "UTC",
              ActComment = "",
              ActivityID = paste0(SiteID, ":", gsub("-","",ActStartDate), ":", ActivityType)) %>%
    dplyr::arrange(SiteID, ActStartDate)%>%
    dplyr::filter(Qualcd != 'P Eqp',
                  Qualcd != 'P Dis',
                  Qualcd != 'P Ssn',
                  Qualcd != 'A e',
                  Qualcd != 'P e')


  #class(nwis.sum.stats.temp.AWQMS$SiteID) <- c("NULL", "number")



  # NWIS DO conc ------------------------------------------------------------


  print("Query NWIS DO begin....")
  nwis.sum.stats.DO <-dataRetrieval::readNWISdv(siteNumbers = USGS_stations,
                                  parameterCd = "00300",
                                  startDate = start.date,
                                  endDate = end.date,
                                  statCd = statcds)

  print("Query NWIS DO end")

  nwis.sum.stats.DO <- dataRetrieval::renameNWISColumns(nwis.sum.stats.DO)


  # Calculate moving averages. If 1 or more daya are missing from period, do not calculate average
  DO4ma <- nwis.sum.stats.DO %>%
    dplyr::group_by(site_no) %>%
    dplyr::arrange(site_no, Date) %>%
    dplyr::mutate(row = dplyr::row_number(),
                  d = runner(x = data.frame(dyMean_run = DO, dyMin_run = DO_Min, datetime = Date),
                             k = "7 days",
                             lag = 0,
                             idx = Date,
                             f = function(x) list(x)),
                  d30 =  runner(x = data.frame(dyMean_run = DO,  datetime = Date),
                                k = "30 days",
                                lag = 0,
                                idx = Date,
                                f = function(x) list(x))) %>%
    dplyr::mutate(d = purrr::map(d, ~ .x %>%
                                   dplyr::summarise(ma.mean7 = dplyr::case_when(length(dyMean_run) >= 6 ~  mean(dyMean_run),
                                                                                TRUE ~ NA_real_),
                                                    ma.min7 = dplyr::case_when(length(dyMin_run) >= 6 ~  mean(dyMin_run),
                                                                               TRUE ~ NA_real_),
                                                    ana_startdate7 = min(datetime),
                                                    ana_enddate7   = max(datetime),
                                                    act_enddate7   = max(datetime))

    ))%>%
    dplyr::mutate(d30 = purrr::map(d30, ~ .x %>%
                                     dplyr::summarise(ma.mean30 = dplyr::case_when(length(dyMean_run) >= 29 ~  mean(dyMean_run),
                                                                                   TRUE ~ NA_real_),
                                                      ana_startdate30 = min(datetime),
                                                      ana_enddate30 =  max(datetime),
                                                      act_enddate30 = max(datetime))

    )) %>%
    tidyr::unnest_wider(d) %>%
    tidyr::unnest_wider(d30) %>%
    dplyr::mutate(ma.mean7 = ifelse(row < 7, NA, ma.mean7),
                  ma.min7 = ifelse(row < 7, NA, ma.min7),
                  ma.mean30 = ifelse(row < 30, NA, ma.mean30)) %>%
    dplyr::select(-row)


  nwis.sum.stats.DO.gather <- DO4ma %>%
    tidyr::gather(DO_Max, DO, DO_Min, ma.mean7, ma.mean30,ma.min7,   key = "stat", value = "result", na.rm = TRUE) %>%
    dplyr::mutate(qual = ifelse(stat == "DO_Max", DO_Max_cd,
                         ifelse(stat == "DO" | stat == "ma.mean7" | stat == "ma.mean30", DO_cd,
                                ifelse(stat == "DO_Min" | stat == "ma.min7", DO_Min_cd, "ERROR" )))) %>%
    dplyr::select(agency_cd, site_no, Date, stat, result, qual, ana_startdate7,ana_enddate7,act_enddate7, ana_startdate30, ana_enddate30, act_enddate30 )


  nwis.sum.stats.DO.AWQMS <- nwis.sum.stats.DO.gather %>%
    dplyr::ungroup() %>%
    dplyr::transmute(CharID = "Dissolved oxygen (DO)",
              Result = round(result, 1),
              Unit = "mg/L",
              Method = "LUMIN",
              RsltType = "Calculated",
              Qualcd = qual,
              StatisticalBasis = dplyr::case_when(stat == "ma.mean7" ~"7DMADMean",
                                           stat == "DO_Max" ~"Daily Maximum",
                                           stat == "DO" ~"Daily Mean",
                                           stat == "DO_Min" ~ "Daily Minimum",
                                           stat == "ma.mean30" ~ "30DMADMean",
                                           stat == "ma.min7" ~ "7DMADMin",
                                           TRUE ~ "ERROR"),
              RsltTimeBasis = ifelse(StatisticalBasis == "7DMADMean" | StatisticalBasis == "7DMADMin", "7 Day",
                                     ifelse(StatisticalBasis == "30DMADMean", "30 Day", "1 Day" )),
              DEQ_RsltComment = ifelse(StatisticalBasis == "7DMADMean" |
                                         StatisticalBasis == "7DMADMin" |
                                         StatisticalBasis == "30DMADMean", "Generated by ORDEQ", "" ),
              ActivityType = "FMC",
              SiteID = site_no,
              SmplColMthd = "ContinuousPrb",
              SmplColEquip = "Probe/Sensor",
              SmplDepth = "",
              SmplDepthUnit = "",
              SmplColEquipComment = "",
              Samplers = "",
              SmplEquipID = "Continuous Probe",
              Project = project,
              ActStartDate = Date,
              ActStartTime = "0:00",
              ActStartTimeZone = "UTC",
              ActEndDate =Date,
              ActEndTime = "0:00",
              ActEndTimeZone = "UTC",
              AnaStartDate = case_when(RsltTimeBasis == "1 Day" ~ format(Date, format="%Y-%m-%d"),
                                       RsltTimeBasis == "7 Day" ~ format(ana_startdate7, format="%Y-%m-%d"),
                                       RsltTimeBasis == "30 Day" ~ format(ana_startdate30, format="%Y-%m-%d")),
              AnaStartTime = case_when(RsltTimeBasis == "1 Day" ~ format(Date, format="%H:%M:%S"),
                                       RsltTimeBasis == "7 Day" ~ format(ana_startdate7, format="%H:%M:%S"),
                                       RsltTimeBasis == "30 Day" ~ format(ana_startdate30, format="%H:%M:%S")),
              AnaStartTimeZone = "UTC",
              AnaEndDate = case_when(RsltTimeBasis == "1 Day" ~ format(Date, format="%Y-%m-%d"),
                                     RsltTimeBasis == "7 Day" ~ format(ana_enddate7, format="%Y-%m-%d"),
                                     RsltTimeBasis == "30 Day" ~ format(ana_enddate30, format="%Y-%m-%d")),
              AnaEndTime = case_when(RsltTimeBasis == "1 Day" ~ format(Date, format="%H:%M:%S"),
                                     RsltTimeBasis == "7 Day" ~ format(ana_enddate7, format="%H:%M:%S"),
                                     RsltTimeBasis == "30 Day" ~ format(ana_enddate30, format="%H:%M:%S")),
              AnaEndTimeZone = "UTC",
              ActComment = "",
              ActivityID = paste0(SiteID, ":", gsub("-","",ActStartDate), ":", ActivityType)) %>%
    dplyr::arrange(SiteID, ActStartDate)%>%
    dplyr::filter(Qualcd != 'P Eqp',
           Qualcd != 'P Dis',
           Qualcd != 'P Ssn',
           Qualcd != 'A e',
           Qualcd != 'P e')

  #class(nwis.sum.stats.DO.AWQMS$SiteID) <- c("NULL", "number")


  # NWIS pH -----------------------------------------------------------------

  print("Query NWIS pH begin....")

  # Split pH pull into 2 seperate pulls
  #
  # When doing the pH data pull, it would throw a "HTTP 414 URI Too Long." error.
  # Modify the pH data pull to split monitoring locations into 2 separate pulls and then combine the data

  station_split <- split(USGS_stations,  c(1,2))



  nwis.cont.ph_1 <- dataRetrieval::readNWISuv(siteNumbers = station_split[[1]],
                                            parameterCd = "00400",
                                            startDate = start.date,
                                            endDate = end.date)

  # Commenting out due to not using contiuous pH for assessment
  nwis.cont.ph_2 <- dataRetrieval::readNWISuv(siteNumbers = station_split[[2]],
                                            parameterCd = "00400",
                                            startDate = start.date,
                                            endDate = end.date)
  nwis.cont.ph <- dplyr::bind_rows(nwis.cont.ph_1, nwis.cont.ph_2)

  print("Query NWIS pH end")




  nwis.cont.ph <- dataRetrieval::renameNWISColumns(nwis.cont.ph)

  nwis_ph_results <- nwis.cont.ph %>%
    #filter out rejected
    dplyr::filter(pH_Inst_cd == "A") %>%
    dplyr::transmute('Monitoring_Location_ID' = site_no,
                     "Activity_start_date" = format(dateTime, "%Y/%m/%d"),
                     'Activity_Start_Time' =format(dateTime, "%H:%M:%S"),
                     'Activity_Time_Zone' = tz_cd,
                     'Characteristic_Name' = "pH",
                     "Result_Value" = pH_Inst,
                     "Result_Unit" = "pH Units",
                     "Result_Status_ID" = pH_Inst_cd)

  #class(nwis_ph_results$Monitoring_Location_ID) <- c("NULL", "number")


#Equipment
  all_data_equipment <- nwis_ph_results %>%
    mutate(Activity_start_date = ymd(Activity_start_date)) %>%
    group_by(Monitoring_Location_ID) %>%
    summarise(min_date = min(Activity_start_date, na.rm = TRUE),
              max_date = max(Activity_start_date, na.rm = TRUE)) %>%
    ungroup() %>%
    mutate(Equipment_ID =Monitoring_Location_ID) %>%
    select(Monitoring_Location_ID, Equipment_ID )

 #class(all_data_equipment$site_no) <- c("NULL", "number")

  pH_deployments <-   nwis_ph_results %>%
    dplyr::left_join(all_data_equipment, by = 'Monitoring_Location_ID') %>%
    dplyr::group_by(Monitoring_Location_ID, Equipment_ID) %>%
    dplyr::summarise(Activity_start_date_time = min(Activity_start_date),
                     Activity_end_date_time  = "",
                     Activity_start_end_time_Zone = dplyr::first(Activity_Time_Zone)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(Media = "Water",
                  Media_subdivision = "Surface Water",
                  Project_ID = project,
                  Alternate_Project_ID = "",
                  Alternate_Project_ID2 = "",
                  Frequency_on_minutes = "",
                  Depth_in_m = "")


  nwis_ph_results_equip <- nwis_ph_results %>%
    left_join(all_data_equipment, by = c('Monitoring_Location_ID')) %>%
    select(Monitoring_Location_ID, Activity_start_date, Activity_Start_Time, Activity_Time_Zone, Equipment_ID,
           Characteristic_Name, Result_Value, Result_Unit, Result_Status_ID)

  nwis_ph_results <- nwis_ph_results_equip

    print("Writing pH files")


  openxlsx::write.xlsx(pH_deployments, file = paste0(save_location, "NWIS_continuous_pH_Deployments.xlsx"))

  data_split_AWQMS(nwis_ph_results, split_on = 'Monitoring_Location_ID', size = 100000, filepath = save_location)


  # Monitoring station information ------------------------------------------


  if (nrow(nwis.sum.stats.DO) > 0 &
      nrow(nwis_ph_results) > 0) {
    nwissites <-
      unique(c(
        unique(nwis.sum.stats.DO.AWQMS$SiteID),
        unique(nwis.sum.stats.temp.AWQMS$SiteID),
        unique(nwis_ph_results$SiteID)
      ))

  } else if (nrow(nwis.sum.stats.DO) > 0 &
             nrow(nwis_ph_results) == 0) {
    nwissites <-
      unique(c(
        unique(nwis.sum.stats.DO.AWQMS$SiteID),
        unique(nwis.sum.stats.temp.AWQMS$SiteID)
      ))

  } else {
    nwissites <- unique(nwis.sum.stats.temp.AWQMS$SiteID)
  }



  nwissites <- dataRetrieval::readNWISsite(unique(nwissites))


  nwis.sites.AWQMS <- nwissites %>%
    dplyr::left_join(county_codes, by = "county_cd") %>%
    dplyr::transmute(Stationkey = site_no,
              Desc = station_nm,
              SiteComments = "",
              MonLocType = dplyr::case_when(site_tp_cd == "ST" ~'River/Stream',
                                            site_tp_cd == "LK" ~ "Lake",
                                            site_tp_cd == "ST-CA" ~"Canal Transport",
                                            site_tp_cd == "GW" ~ "Well",
                                            site_tp_cd == "WE" ~ "Wetland",
                                            site_tp_cd == "AT" ~ "Atmosphere",
                                            TRUE ~ "ERROR" ),
              COUNTY = county_nm,
              STATE = "OR",
              Country = "US",
              HUC8 = huc_cd,
              HUC12 = "",
              TribalLand = "",
              TribalName = "",
              CreateDate = "",
              T_R_S = "",
              Lat = dec_lat_va,
              Long = dec_long_va,
              Datum = dec_coord_datum_cd,
              CollMethod = "Interpolation-Digital Map Source",
              MapScale = map_scale_fc,
              Comments = "",
              WellType = ifelse(MonLocType == "Well", "Monitoring", ""),
              WellForm = "",
              WellAquiferName = ifelse(MonLocType == "Well", nat_aqfr_cd, ""),
              WellDepth = ifelse(MonLocType == "Well", well_depth_va, ""),
              WellDepthUnit = "ft",
              AltLocID = "",
              AltLocName = "",
              EcoRegion3 = "",
              EcoRegion4 = "",
              Reachcode = "",
              GNIS_Name = "",
              AU_ID = ""
    )

  # remove NAs
  nwis.sites.AWQMS[is.na(nwis.sites.AWQMS)] <- ""

  #class(nwis.sites.AWQMS$Stationkey) <- c("NULL", "number")

  write.csv(nwis.sites.AWQMS, paste0(save_location,"NWIS_Monitoring_Locations.csv"), row.names = FALSE)



# Write sumstat files ---------------------------------------------------------------------------------------------


  NWIS_sum_stats_data <- dplyr::bind_rows(nwis.sum.stats.temp.AWQMS
                         ,nwis.sum.stats.DO.AWQMS)




  if(check_dups){

  print("Check for summary statistics duplicates")
  duplicate_check <- dup_check_AWQMS(NWIS_sum_stats_data)

  AWQMS_import <- duplicate_check[["non_duplicates"]]
  suspected_dups <- duplicate_check[["suspected_dups"]]
  suspected_updates <- duplicate_check[["suspected_updates"]]

  print('Writing Sumstat files')

  if(split_file){
    data_split_AWQMS(AWQMS_import, split_on = "SiteID", size = 100000, filepath = save_location)
  } else {
  write.csv(NWIS_data, paste0(save_location,"NWIS_sum_stats-", start.date, " - ", end.date, ".csv"), row.names = FALSE)
  }

write.csv(suspected_dups, paste0(save_location,"NWIS_suspected_duplcates-", start.date, " - ", end.date, ".csv"), row.names = FALSE)
write.csv(suspected_updates, paste0(save_location,"NWIS_suspected_updates-", start.date, " - ", end.date, ".csv"), row.names = FALSE)

  } else {

    print('Writing Sumstat files')

    if(split_file){
      data_split_AWQMS(NWIS_sum_stats_data, split_on = "SiteID", size = 100000, filepath = save_location)
    } else {
      write.csv(NWIS_sum_stats_data, paste0(save_location,"NWIS_sum_stats-", start.date, " - ", end.date, ".csv"), row.names = FALSE)
    }
}


  con_data_list <-list(  sumstats=as.data.frame(NWIS_sum_stats_data),
                         pH_continuous =as.data.frame(nwis_ph_results),
                         pH_deployments = as.data.frame(pH_deployments),
                         monitoring_locations = as.data.frame(nwis.sites.AWQMS))


  return(con_data_list)



}

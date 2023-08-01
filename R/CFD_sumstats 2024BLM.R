#' CFD_sumstats_2024_BLM
#'
#' Calculates summary statistics from BLM excel exports in the 2024 CFD, and creates AWQMS import files.
#' Summary stats file will be saved at the same location ans input file.
#'
#' @import dplyr
#' @import lubridate
#' @import openxlsx
#' @import zoo
#' @import purrr
#' @param project Name of project for AWQMS
#' @param type How to select files. "file" lets you specify path in function arguments, 'file_select' brings up a chooser window, "directory" will loop through all files in a directory.
#' @param path filepath for file to process, if type == "path"
#' @import dplyr
#' @importFrom runner runner
#' @export


CFD_sumstats_2024_BLM <- function(project, path = NULL, is_salem = FALSE){

  options(dplyr.summarise.inform = FALSE)


# Testing -----------------------------------------------------------------
#
  # project = 'IR Call for Data'
  # type = "file"
  # path = "C:/Users/tpritch/Oregon/DEQ - Integrated Report - IR 2024/CallForData/2024/Submitted Data/Archive of Original Files/BLM/excel_versions/BLM_Data_Burns.xlsx"
  # is_salem = FALSE

# Error checking --------------------------------------------------------------------------------------------------



  if(!(is.character(project) & length(project) == 1)){
    stop("project must be a charcter of length 1")
  }


  if(is.null(path)){

    stop("path must be specified")
  }


# Filepath select -------------------------------------------------------------------------------------------------


    filepath <-  path




# Read file -------------------------------------------------------------------------------------------------------

# Project Info ----------------------------------------------------------------------------------------------------

  if(is_salem){
    alternate_project_ID <- 'CoSContinuousWQ'

  } else {

    project_import <-   readxl::read_xlsx(filepath, sheet = "Projects")

    alternate_project_ID <- project_import[[1,1]]

  }



# Results ---------------------------------------------------------------------------------------------------------



excel_sheets <- readxl::excel_sheets(path)
Result_sheets <- excel_sheets[str_detect(excel_sheets, 'Results', negate = FALSE)]

read_blm_results <- function(results_sheet){
  # read results tab of submitted file
  part_data <- readxl::read_xlsx(filepath, sheet = results_sheet) |>
    dplyr::select(-OBJECTID, -TEMP_GUID, -SAMPLE_GUID, -BLM_ORG_CD)

  colnames(part_data) <- c("Monitoring_Location_ID",
                                "Activity_Start_Date",
                                "Activity_Start_Time",
                                "Activity_Time_Zone",
                                "Characteristic_Name",
                                "Equipment_ID",
                                "Result_Value",
                                "Result_Unit",
                                "Result_Status_ID")
  return(part_data)
}

print("Reading Result Sheets")
Results_import <- map_dfr(Result_sheets, read_blm_results, .progress = TRUE) |>
  dplyr::mutate(Characteristic_Name = case_when(Characteristic_Name == 'Temperature,water' ~ 'Temperature, water',
                                                TRUE ~ Characteristic_Name))
print("Finished importing Results")



# convert F to C, filter out rejected data, and create datetime column
results_data <- Results_import %>%
  mutate(r = ifelse(Result_Unit == "deg F", round((Result_Value - 32)*(5/9),2), Result_Value),
         r_units = ifelse(Result_Unit == "deg F", "deg C", Result_Unit )) %>%
  filter(Result_Status_ID != "Rejected") %>%
  mutate(datetime = paste(as.Date(Activity_Start_Date), Activity_Start_Time   )) |>
  mutate(datetime = lubridate::ymd_hm(datetime),
         Activity_Start_Date = as.Date(Activity_Start_Date))


# Deployment info ---------------------------------------------------------

deployments <- results_data %>%
  group_by(Monitoring_Location_ID, Equipment_ID, ) %>%
  summarise(startdate = min(datetime),
            enddate = max(datetime) + lubridate::minutes(5),
            TZ = first(Activity_Time_Zone))

write.csv(deployments, paste0(tools::file_path_sans_ext(filepath),"-deployments.csv"), row.names = FALSE)



# Summary Stats -----------------------------------------------------------



# get unique list of characteristics to run for loop through
unique_characteritics <- unique(Results_import$Characteristic_Name)


#create list for getting data out of loop
monloc_do_list <- list()
sumstatlist <- list()

# For loop for summary statistics -----------------------------------------

# Loop goes through each characteristc and generates summary stats
# After loop, data gets pushed inot single table
for (i in 1:length(unique_characteritics)){

  print(paste("Begin",  unique_characteritics[i], "- characteristic", i, "of", length(unique_characteritics)))

  # Characteristic for this loop iteration
  char <- unique_characteritics[i]

  # Filter so table only contains single characteristic
  results_data_char <- results_data %>%
    filter(Characteristic_Name == char) %>%
    # generare unique hour field for hourly values and stats
    mutate(hr =  format(datetime, "%Y-%j-%H"),
           mloc_equip = paste0(Monitoring_Location_ID, "-", Equipment_ID))

  # Simplify to hourly values and Stats
  hrsum <- results_data_char %>%
    group_by(Monitoring_Location_ID, Equipment_ID, hr, r_units, Activity_Time_Zone, Result_Unit, mloc_equip) %>%
    summarise(date = mean(Activity_Start_Date),
              hrDTmin = min(datetime),
              hrDTmax = max(datetime),
              hrN = sum(!is.na(r)),
              hrMean = mean(r, na.rm=TRUE),
              hrMin = min(r, na.rm=TRUE),
              hrMax = max(r, na.rm=TRUE))


  # For each date, how many hours have hrN > 0
  # remove rows with zero records in an hour.
  hrdat<- hrsum[which(hrsum$hrN >0),]

  daydat <- hrdat %>%
    dplyr::group_by(Monitoring_Location_ID, Equipment_ID, date, Result_Unit, Activity_Time_Zone, mloc_equip) %>%
    dplyr::summarise(dDTmin = min(hrDTmin),
                     dDTmax = max(hrDTmax),
                     hrNday = length(hrN),
                     dyN = sum(hrN),
                     dyMean =mean(hrMean, na.rm=TRUE),
                     dyMin = min(hrMin, na.rm=TRUE),
                     dyMax = max(hrMax, na.rm=TRUE)) %>%
    dplyr::mutate(ResultStatusID = dplyr::if_else(hrNday >= 22, 'Final', "Rejected"),
                  cmnt = dplyr::case_when(hrNday >= 22 ~ "Generated by ORDEQ",
                                          hrNday <= 22 & hrNday >= 20 ~ paste0("Generated by ORDEQ; Estimated - ", as.character(hrNday), ' hrs with valid data in day'),
                                          TRUE ~ paste0("Generated by ORDEQ; Rejected - ", as.character(hrNday), ' hrs with valid data in day')))


  #Deal with DO Results
  if (results_data_char$Characteristic_Name[1] == "Dissolved oxygen (DO)") {

    #Filter dataset to only look at 1 monitoring location at a time
    daydat_station <- daydat %>%
      dplyr::filter(hrNday >= 22) %>%
      dplyr::filter(ResultStatusID != "Rejected")


      daydat_station2 <- daydat_station %>%
        dplyr::ungroup() %>%
        dplyr::group_by(mloc_equip) %>%
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
      sum_stats <- daydat_station2 %>%
        dplyr::arrange(Monitoring_Location_ID, Equipment_ID, date)


  } # end of DO if statement


  ##  TEMPERATURE
  if (results_data_char$Characteristic_Name[1] == 'Temperature, water' ) {



    #monitoring location loop

    #Filter dataset to only look at 1 monitoring location at a time
    daydat_station <- daydat %>%
      dplyr::ungroup() %>%
      dplyr::group_by(mloc_equip) %>%
      dplyr::filter(hrNday >= 22)


    # 7 day loop
    # Loops through each row in the monitoring location dataset
    # And pulls out records that are within the preceding 7 day window
    # If there are at least 6 values, then calculate 7 day min and mean
    # Assigns data back to daydat_station
    print("Begin 7 day moving averages")



    daydat_station2 <- daydat_station %>%
      dplyr::ungroup() %>%
      dplyr::group_by(mloc_equip) %>%
      dplyr::mutate(row = dplyr::row_number(),
                    d = runner(x = data.frame(dyMax_run = dyMax,  dDTmin_run = dDTmin,
                                              dDTmax_run = dDTmax),
                               k = "7 days",
                               lag = 0,
                               idx = date,
                               f = function(x) list(x))) %>%
      dplyr::mutate(d = purrr::map(d, ~ .x %>%
                                     dplyr::summarise(ma.max7 = dplyr::case_when(length(dyMax_run) >= 6 ~  mean(dyMax_run),
                                                                                 TRUE ~ NA_real_
                                     ),
                                     ana_startdate7 = min(dDTmin_run),
                                     ana_enddate7 =  max(dDTmax_run),
                                     act_enddate7 = max(dDTmax_run))

      ))%>%
      tidyr::unnest_wider(d) %>%
      dplyr::mutate(ma.max7 = ifelse(row < 7, NA, ma.max7)) %>%
      dplyr::select(-row)




    # Combine list to single dataframe
    sum_stats <- daydat_station2 %>%
      dplyr::arrange(Monitoring_Location_ID, Equipment_ID, date)




  } #end of temp if statement



  ## Other - just set sum_stats to daydat, since no moving averages need to be generated.
  if (results_data_char$Characteristic_Name[1] != 'Temperature, water' & results_data_char$Characteristic_Name[1] != "Dissolved oxygen (DO)"  ) {

    sum_stats <- daydat

  } #end of not DO or temp statement

  #Assign the char ID to the dataset
  sum_stats <- sum_stats %>%
    mutate(charID = char)

  #Set to list for getting out of for loop
  sumstatlist[[i]] <-  sum_stats


} # end of characteristics for loop



# Bind list to dataframe
sumstat <- bind_rows(sumstatlist)


#add any missing columns
sumstat_long_cols <- c("ana_startdate7", "ana_startdate30", "ana_enddate7", "ana_enddate30", "ma.max7_DQL",
                       "ma.min7_DQL", "ma.mean7_DQL", "ma.mean30_DQL", "ma.max7", 'ma.min7', 'ma.mean7', 'ma.mean30')

missing_cols <- setdiff(sumstat_long_cols, names(sumstat))

sumstat[missing_cols] <- NA

#Gather summary statistics from wide format into long format
#rename summary statistcs to match AWQMS Import COnfiguration
sumstat_long <- sumstat %>%
  rename("Daily Maximum" = dyMax,
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
  arrange(Monitoring_Location_ID, date) %>%
  rename(Equipment = Equipment_ID)


# Read Audit Data ---------------------------------------------------------
#
# Audit_import <- read_excel(filepath, sheet = "Audit_Data", col_types = c("guess",
#                            "guess", "guess", "guess", "guess", "date", "guess",
#                            'date',"guess", "guess", "guess", "guess", "guess",
#                            "guess", "guess", "guess", "guess", "guess", "guess",
#                            "guess", "guess" ))
#
# colnames(Audit_import) <- make.names(names(Audit_import), unique=TRUE)
#
# # get rid of extra blankfields
# Audits <- Audit_import %>%
#   filter(!is.na(Project.ID))
#
# # table of methods unique to project, location, equipment, char, and method
# Audits_unique <- unique(Audits[c("Project.ID", "Monitoring_Location_ID", "Equipment_ID", "Characteristic_Name", "Result.Analytical.Method.ID")])
#
#
#
# # Reformat Audit info
# matches Dan Brown's import configuration
# If template has Result.Qualifier as column, use that value, if not use blank.
# Audit_info <- Audits %>%
#   mutate(Result.Qualifier = ifelse("Result.Qualifier" %in% colnames(Audits), Result.Qualifier, "" ),
#          Activity_Start_Time = as.character(strftime(Activity_Start_Time, format = "%H:%M:%S", tz = "UTC")),
#          Activity.End.Time = as.character(strftime(Activity.End.Time, format = "%H:%M:%S", tz = "UTC")) ) %>%
#   select(Project.ID, Monitoring_Location_ID, Activity_Start_Date,
#          Activity_Start_Time, Activity.End.Date, Activity.End.Time,
#          Activity_Time_Zone, Activity.Type,
#          Activity.ID..Column.Locked., Equipment_ID, Sample.Collection.Method,
#          Characteristic_Name, Result.Value, Result_Unit, Result.Analytical.Method.ID,
#          Result.Analytical.Method.Context, Result.Value.Type, Result.Status.ID,
#          Result.Qualifier, Result.Comment)

#Write excel file for AWQMS import
#write.xlsx(Audit_info, file = paste0(tools::file_path_sans_ext(filepath),"-Audits.xlsx") )



# AWQMS summary stats -----------------------------------------------------


# Join method to sumstat table
# sumstat_long <- sumstat_long %>%
#   mutate(Equipment = as.character(Equipment)) %>%
#   left_join(Audits_unique, by = c("Monitoring_Location_ID", "charID" = "Characteristic_Name") )
AQWMS_sum_stat <- sumstat_long %>%
  ungroup() %>%
  mutate(RsltTimeBasis = ifelse(StatisticalBasis == "7DMADMin" |
                                  StatisticalBasis == "7DMADMean" |
                                  StatisticalBasis == "7DMADMax", "7 Day",
                                ifelse(StatisticalBasis == "30DMADMean", "30 Day", "1 Day" )),
         ActivityType = "FMC",
         Result.Analytical.Method.ID = ifelse(charID == "Conductivity", "120.1",
                                              ifelse(charID == "Dissolved oxygen (DO)" |
                                                       charID == "Dissolved oxygen saturation", "NFM 6.2.1-LUM",
                                                     ifelse(charID == "pH","150.1",
                                                            ifelse(charID == "Temperature, water", "170.1",
                                                                   ifelse(charID == "Turbidity", "180.1", "error" ))))),
         SmplColMthd = "ContinuousPrb",
         SmplColEquip = "Probe/Sensor",
         SmplDepth = "",
         SmplDepthUnit = "",
         SmplColEquipComment = "",
         Samplers = "",
         Project = project,
         alt_project = alternate_project_ID,
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
         ActStartDate = date,
         ActStartTime = "0:00",
         ActEndDate = AnaEndDate,
         ActEndTime = AnaEndTime,
         RsltType = "Calculated",
         ActStartTimeZone = Activity_Time_Zone,
         ActEndTimeZone = Activity_Time_Zone,
         AnaStartTimeZone = Activity_Time_Zone,
         AnaEndTimeZone = Activity_Time_Zone,
         Result = round(Result, digits = 2)
  ) %>%
  select(charID,
         Result,
         Result_Unit,
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
         alt_project,
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
         AnaEndTimeZone)

# Export to same place as the originial file
openxlsx::write.xlsx(AQWMS_sum_stat, paste0(tools::file_path_sans_ext(filepath),"-statsum.xlsx"))



# Cont. pH --------------------------------------------------------------------------------------------------------

cont_Ph <- results_data %>%
  filter(Characteristic_Name == "pH")



cont_pH_AWQMS <- cont_Ph %>%
  dplyr::transmute('Monitoring_Location_ID' = Monitoring_Location_ID ,
                   "Activity_start_date" = format(datetime, "%Y/%m/%d"),
                   'Activity_Start_Time' =format(datetime, "%H:%M:%S"),
                   'Activity_Time_Zone' = Activity_Time_Zone,
                   'Equipment_ID' = Monitoring_Location_ID ,
                   'Characteristic_Name' = Characteristic_Name ,
                   "Result_Value" = Result_Value,
                   "Result_Unit" = Result_Unit,
                   "Result_Status_ID" = Result_Status_ID)

if(nrow(cont_pH_AWQMS) > 0){
# Export to same place as the originial file
openxlsx::write.xlsx(cont_pH_AWQMS, paste0(tools::file_path_sans_ext(filepath),"-cont_pH.xlsx"))
}

# Graphing ----------------------------------------------------------------


graph <- ggplot2::ggplot(results_data, ggplot2::aes(x = as.factor(Monitoring_Location_ID), y = r) )+
  ggplot2::geom_boxplot(fill = "gray83") +
  ggplot2::geom_jitter(width = 0.2, alpha = 0.1, color = "steelblue4") +
  ggplot2::facet_grid(Characteristic_Name ~ ., scales = 'free') +
  ggplot2::theme_bw() +
  ggplot2::xlab("Monitoring Location") +
  ggplot2::ylab("Result") +
  ggplot2::theme(strip.background = ggplot2::element_blank())


ggplot2::ggsave(paste0(tools::file_path_sans_ext(filepath),"-Graph.png"), plot = graph)



}



#' Remove duplicates from NWIS files
#'
#' This function will read in the NWIS summary statistic import files and
#' remove results found in AWQMS already. This significantly cuts down on AWQMS
#' load times and potential for erroring out.
#'
#' @param startdate Start date of IR data window. Used to reducde size of AWQMS pull
#' @param enddate End date of IR data window
#' @author Travis Pritchard
#' @import tidyverse
#' @export

remove_NWIS_dups <- function(filename,
                             startdate = '2020/1/1',
                             enddate = '2024/12/31'
                             ){
  # library(tidyverse)
  # library(openxlsx)
  # library(AWQMSdata)
  # library(DBI)


  # Get constants -----------------------------------------------------------
readRenviron("~/.Renviron")




# Read in import file -----------------------------------------------------

# This section pulls up a file picker and allows the user to choose a file
  # It finds the filepath, the filename, and the orginical directory
  # It creates a new filepath for the generated files in a sub directory
  # Named "/no_dups/". This directory must exist

  if(filename == "Choose"){

file <- tcltk::tk_choose.files(caption = "Choose File")

  } else {

    file = filename
}
filename <- basename(file)
directory <- dirname(file)
newfilename <- paste0(strsplit(filename, "[.]")[[1]][1], "-no_dups.xlsx")
new_path <- paste0(directory, '/no_dups/', newfilename)



# Read in import file -----------------------------------------------------

print("Read import File.")
import_file <- openxlsx::read.xlsx(file,
                         detectDates = TRUE)



# Connect to AWQMS db -----------------------------------------------------


print("Get existing AWQMS data")
con <- DBI::dbConnect(odbc::odbc(), 'AWQMS-cloud',
                      UID      =   Sys.getenv('AWQMS_usr'),
                      PWD      =  Sys.getenv('AWQMS_pass'))

# stat_base <- dplyr::tbl(con, 'results_deq_vw') |>
#   select(Statistical_Base) |>
#   distinct() |>
#   collect() |>
#   pull()


# AWQMS data pull ---------------------------------------------------------

# This section will query AWQMS and get data that is labeled as:
  # OrganizationID: 'USGS-OR(INTERNAL)'
  # Mlocs: Present in import file
  # SampleStartDate: Inbewteen IR data window
AWQMs_exist <- dplyr::tbl(con, 'results_deq_vw') |>
  dplyr::filter(OrganizationID == 'USGS-OR(INTERNAL)',
                MLocID %in% import_file$SiteID,
                as.Date(SampleStartDate) >= as.Date(startdate) &
                  as.Date(SampleStartDate) <= as.Date(enddate) ) |>
  dplyr::select(MLocID, act_id, #Activity_Type,
         SampleStartDate,#SampleStartTime,
         Char_Name, Result_Numeric,Result_Unit,
         Statistical_Base) |>
  dplyr::collect() |>
  dplyr::rename(SiteID = MLocID,
         ActivityID = act_id,
         #ActivityType = Activity_Type,
         ActStartDate = SampleStartDate,
         #ActStartTime = SampleStartTime,
         CharID =  Char_Name,
         Result = Result_Numeric,
         Unit = Result_Unit,
         StatisticalBasis = Statistical_Base) |>
  dplyr::mutate(in_AWQMS = 1,
         ActStartDate = as.Date(ActStartDate),
         StatisticalBasis = case_when(StatisticalBasis == 'Maximum' ~ "Daily Maximum",
                                      StatisticalBasis == 'Mean' ~ "Daily Mean",
                                      StatisticalBasis == 'Minimum' ~ "Daily Minimum",
                                      StatisticalBasis == '7DADM' ~ "7DMADMax",
                                      StatisticalBasis == '7DADMean' ~ "7DMADMean",
                                      StatisticalBasis == '7DADM' ~ "7DMADMin",
                                      StatisticalBasis == '30DADMean' ~ "30DMADMean",
                                      TRUE ~ StatisticalBasis))



# Join and filter out non matching joins ----------------------------------


print("Compare and drop duplicates")

joined <- import_file |>
  left_join(AWQMs_exist, by = join_by(CharID, Result, Unit, StatisticalBasis,
                                      SiteID, ActStartDate, ActivityID)) |>
  filter(is.na(in_AWQMS)) |>
  select(-in_AWQMS)



# Write new xlsx file -----------------------------------------------------


print("Write xlsx")
write.xlsx(joined, file = new_path)

}

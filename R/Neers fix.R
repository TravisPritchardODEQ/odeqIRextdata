
library(tidyverse)
library(openxlsx)


NERRs_fix_equipment <- function(file){


  filename <- basename(file)
  directory <- dirname(file)
  newfilename <- paste0(strsplit(filename, "[.]")[[1]][1], "-fixed_equipment.xlsx")
  new_path <- paste0(directory, '/fix_equip/', newfilename)



  import_file <- openxlsx::read.xlsx(file,
                                     detectDates = TRUE)


  fixed <- import_file |>
    mutate(Equipment_ID = Monitoring_Location_ID)


  write.xlsx(fixed, file = new_path)

}



files <- list.files('C:/Users/tpritch/OneDrive - Oregon/DEQ - Integrated Report - IR_2026/DataAssembly/NERRS/Upload Files/',
                    full.names =  TRUE)

files <- files[ str_detect(files, 'cont_pH')]


map(files, NERRs_fix_equipment, .progress = TRUE)




# Deal with deployments ---------------------------------------------------



# Get existing AWQMS equipment --------------------------------------------

con <- DBI::dbConnect(odbc::odbc(), 'AWQMS-cloud',
                      UID      =   Sys.getenv('AWQMS_usr'),
                      PWD      =  Sys.getenv('AWQMS_pass'))


AWQMS_nwis_equipment <- dplyr::tbl(con, 'continuous_results_deq_vw') |>
  filter(OrganizationID == 	'USGS-OR(INTERNAL)') |>
  select(MLocID, Equipment_ID) |>
  distinct() |>
  arrange(MLocID) |>
  collect()


# read in equip_deploysheet -----------------------------------------------


NWIS_deployments <- read.xlsx('C:/Users/tpritch/OneDrive - Oregon/DEQ - Integrated Report - IR_2026/DataAssembly/NWIS/NWIS_continuous_pH_Deployments.xlsx')

AWQMS_nwis_equipment <- AWQMS_nwis_equipment |>
  mutate(in_awqms = 1)


joined <- NWIS_deployments |>
  left_join(AWQMS_nwis_equipment, by = join_by(Monitoring_Location_ID ==MLocID )) |>
  filter(is.na(in_awqms))

NWIS_deployments_new <- NWIS_deployments |>
  filter(Monitoring_Location_ID %in% joined$Monitoring_Location_ID) |>
  mutate(Equipment_ID = Monitoring_Location_ID,
         Activity_end_date_time = "")

write.xlsx(NWIS_deployments_new, file = 'C:/Users/tpritch/OneDrive - Oregon/DEQ - Integrated Report - IR_2026/DataAssembly/NWIS/NWIS_continuous_pH_Deployments_newonly.xlsx')


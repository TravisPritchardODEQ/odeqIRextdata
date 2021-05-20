library(odeqIRextdata)
library(purrr)
library(dplyr)


# Install ---------------------------------------------------------------------------------------------------------

devtools::install_github('TravisPritchardODEQ/odeqIRextdata', upgrade = 'never')


# Call for data ---------------------------------------------------------------------------------------------------


# Enter file- For when you want to specify a file
CFD_sumstats(project = 'Integrated Report – Call for Data',
             type = "file",
             path = "C:/Users/tpritch/Documents/Test CFD files/ooi/WorkingCopy-ContinuousSubmission_EA_Data.xlsx")

# File select- This will bring up a window to select which file you want to process
CFD_sumstats(project = 'Integrated Report – Call for Data', type = "file_select")

# Whole directory- When you want to process a whole folder. Will bring up a section window.
CFD_sumstats(project = 'Integrated Report – Call for Data', type = "directory")

#################################################################################
# If City of Salem, who didn't use the project tab, use is_salem = TRUE. Example:
#################################################################################

CFD_sumstats(project = '2022 IR Call for Data', type = "file_select", is_salem = TRUE)




# NWIS ------------------------------------------------------------------------------------------------------------

a <- Sys.time()
NWIS_data <- NWIS_cont_data_pull(start.date = '2016-01-01',
                    end.date = "2020-12-31",
                    save_location = 'C:/Users/tpritch/Documents/NWIS data/',
                    project = 'Integrated Report – Call for Data',
                    check_dups = FALSE)
Sys.time() - a

save(NWIS_data, file = 'C:/Users/tpritch/Documents/NWIS data/NIWS_data.RData')



# Portland BES data -------------------------------------------------------

# This file came from Ryan Michie
BES_inventory_import <- read.csv("C:/Users/tpritch/Documents/odeqIRextdata/PortlandBes_data_inventory.csv")


BES_inventory <- BES_inventory_import %>%
  transmute(station = LocationIdentifier,
            startdate = '2016-01-01',
            enddate = '2020-12-31',
            char = gsub("@.*$","",Identifier)) %>%
  filter(char %in% c('Dissolved oxygen.Primary',
                     'pH.Primary',
                     'Temperature.Primary', 'Temperature.7DADM'))

BES_stations <- BES_inventory_import %>%
  select(LocationIdentifier,LocationName, Latitude, Longitude, LocationType ) %>%
  distinct()


write.csv(BES_stations, file = "BES_stations.csv")

a <- Sys.time()
Portland_BES_data <- copbes_AWQMS(BES_inventory, "Integrated Report – Call for Data")
Sys.time() - a


deployments <- Portland_BES_data[['deployments']]
sumstats <- Portland_BES_data[['sumstats']]
pH_cont <- Portland_BES_data[['pH_continuous']]
pH_deployments <- Portland_BES_data[['pH_deployments']]

data_split_AWQMS(sumstats, split_on = "Monitoring_Location_ID", size = 100000,
                 filepath = '//deqlab1/Assessment/Integrated_Report/DataSources/2022/City of portland- continuous/')


data_split_AWQMS(pH_cont, split_on = "Monitoring_Location_ID", size = 100000,
                 filepath = '//deqlab1/Assessment/Integrated_Report/DataSources/2022/City of portland- continuous/')

write.csv(pH_deployments, file = '//deqlab1/Assessment/Integrated_Report/DataSources/2022/City of portland- continuous/pH_deployments.csv',
          row.names = FALSE)


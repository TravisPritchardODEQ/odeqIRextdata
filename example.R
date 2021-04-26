library(odeqIRextdata)
library(purrr)



# Call for data ---------------------------------------------------------------------------------------------------


# Enter file- For when you want to specify a file
CFD_sumstats(project = '2022 IR Call for Data',
             type = "file",
             path = "C:/Users/tpritch/Documents/Test CFD files/CoS_ContinuousWQ_01012018-02062020_CopyforTravis.xlsx")

# File select- This will bring up a window to select which file you want to process
CFD_sumstats(project = '2022 IR Call for Data', type = "file_select")

# Whole directory- When you want to process a whole folder. Will bring up a section window.
CFD_sumstats(project = '2022 IR Call for Data', type = "directory")



# NWIS ------------------------------------------------------------------------------------------------------------

a <- Sys.time()
NWIS_cont_data_pull(start.date = '2019-01-01',
                    end.date = "2019-12-31",
                    save_location = 'C:/Users/tpritch/Documents/Test CFD files/',
                    project = '2022 IR Call for Data' )
Sys.time() - a

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
                     'Specific conductance.Primary',
                     'Temperature.Primary', 'Temperature.7DADM'))
BES_stations <- BES_inventory_import %>%
  select(LocationIdentifier,LocationName, Latitude, Longitude, LocationType ) %>%
  distinct()


a <- Sys.time()
Portland_BES_data <- copbes_AWQMS(BES_inventory, "2022 call for data")
Sys.time() - a


deployments <- Portland_BES_data[['deployments']]
sumstats <- Portland_BES_data[['sumstats']]
pH_cont <- Portland_BES_data[['pH_continuous']]



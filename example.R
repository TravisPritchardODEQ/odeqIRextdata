library(odeqIRextdata)
library(purrr)

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



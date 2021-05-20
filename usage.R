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




# OWRD  -------------------------------------------------------------------------------------------------------

#
station=c("14335500","14342500","14104800",
          "14335200", "14335230","14335300","14343000",
          "14346700","14354100","14347800","14336700",
          "14337000","14368300")
startdate="2016-01-01"
enddate="2020-12-31"
char=c("WTEMP_MAX")

a <- owrd_data(station, startdate, enddate, char)


station =c("14104190","14032400","14031050","14357503",
           "14021000","14358725","14348080","14335235",
           "14348000","14335250","14320700","14341610",
           "14348150","14352001","14355875","14358750",
           "14022500","14023500","14350900","14352000",
           "14025000","14026000","14365500","14363450",
           "14360500","14340800","14357000","14354950",
           "14327122","14192500","14358800","13330500",
           "14358680")

b <- owrd_data(station, startdate, enddate, char)

station =c("14306900","14193000","11495900","11494000",
           "14350000","14076100","14029900","14348400",
           "11504109","14327137","11510000","13330300",
           "14024100","14063000","14081500","13275105",
           "14024300","14060000","14358610","14088500",
           "14079800","14083400","14085700","11495600",
           "13275300","14073520","14075000","14087300",
           "14082550","14375200","11504040","11504103",
           "13329100","13216500")

c <- owrd_data(station, startdate, enddate, char)

station =c("14095250","14095255","14306820","14080500",
           "14400200","14202850","13273000","14202510",
           "14054000","14076020","11497100","14074900",
           "14070920","13331450","13330000","14070980",
           "13325500","14327120","11502550","13214000",
           "13269450","11500400","13317850","13329765",
           "13215000","13217500","11504120","14346900",
           "11497550","11500500","11494510","11497500",
           "14064500","14056500","14073000","13318210",
           "11491400","13282550","13318060","13318920",
           "14039500","10392400","14327300","11503500",
           "14031600","14010800","14032000","14105550",
           "14104700","14105545")

d <- owrd_data(station, startdate, enddate, char)

all <- rbind(a,b,c,d)

all_sum <- all %>%
  group_by(published_status) %>%
  summarise(n= n(),
            per_status = round(((n/205595)*100),digits = 2))

# Plot percentages
ggplot(data=all_sum, aes(x=published_status, y=per_status)) +
  geom_bar(stat="identity", fill="steelblue")+
  geom_text(aes(label=per_status), vjust=-0.3, size=3.5)+
  theme_minimal()

all_pub <- all %>%
  filter(published_status == 'PUB')

stations_pub <- all_pub %>%
  select(station_nbr) %>%
  distinct()

save.image(file = "//deqlab1/Assessment/Integrated_Report/DataSources/2022/OWRD/all_data.RData")


#format to AWQMS

OWRD_temp_sumstats <- owrd_AWQMS(all_pub, project = 'Integrated Report – Call for Data')


data_split_AWQMS(OWRD_temp_sumstats, split_on = "SiteID", size = 100000,
                 filepath = '//deqlab1/Assessment/Integrated_Report/DataSources/2022/OWRD/')

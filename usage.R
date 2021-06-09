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



# Air temperature -------------------------------------------------------------------------------------------------

#These are the air stations we used in 2018/2020

air_temp_stations <-c(
  "USC00350036",
  "USR0000OAGN",
  "USC00350118",
  "USR0000OALL",
  "USC00350145",
  "USS0017D02S",
  "USS0022G06S",
  "USC00350197",
  "USR0000OANT",
  "USS0019D02S",
  "USC00350265",
  "USC00350304",
  "USW00094224",
  "USW00094281",
  "USR0000OBAD",
  "USW00024130",
  "USR0000OBKL",
  "USR0000OBAL",
  "USC00350471",
  "USC00350501",
  "USR0000OBAS",
  "USS0018D09S",
  "USC00350652",
  "USC00350699",
  "USC00350694",
  "USS0022G21S",
  "USS0023G15S",
  "USS0022G13S",
  "USR0000OBLA",
  "USS0021D33S",
  "USR0000OBLU",
  "USS0018E16S",
  "USR0000OBOC",
  "USR0000OBOH",
  "USC00350858",
  "USC00350897",
  "USR0000OBOU",
  "USS0018E05S",
  "USS0018D20S",
  "USR0000OBRI",
  "USC00351058",
  "USR0000OBRO",
  "USR0000OBRU",
  "USR0000OBUE",
  "USR0000OBUS",
  "USW00094185",
  "USR0000OCAB",
  "USR0000OCAI",
  "USR0000OCAL",
  "USR0000OCAN",
  "USS0022F03S",
  "USR0000OCAS",
  "USS0021F22S",
  "USC00351546",
  "USR0000OCHI",
  "USR0000OCIN",
  "USS0021D13S",
  "USC00351643",
  "USS0021D12S",
  "USC00351682",
  "USS0018D08S",
  "USS0022G24S",
  "USR0000OCOD",
  "USR0000OCOL",
  "USC00351765",
  "USW00004141",
  "USC00351836",
  "USW00004236",
  "USC00351862",
  "USC00351877",
  "USC00351897",
  "USC00351902",
  "USC00351914",
  "USR0000OCRA",
  "USC00351946",
  "USS0020G12S",
  "USR0000OCRO",
  "USC00352112",
  "USS0022E08S",
  "USC00352135",
  "USC00352173",
  "USS0019E03S",
  "USC00352292",
  "USS0022F18S",
  "USC00352374",
  "USC00352406",
  "USR0000ODUN",
  "USR0000OEAG",
  "USR0000OEDE",
  "USS0018E03S",
  "USR0000OECK",
  "USC00352632",
  "USC00352633",
  "USR0000OEMI",
  "USS0018D04S",
  "USC00352693",
  "USW00024221",
  "USR0000OEVA",
  "USR0000OFAL",
  "USC00352867",
  "USR0000OFIE",
  "USR0000OFIN",
  "USS0018G02S",
  "USR0000OFIS",
  "USS0022G14S",
  "USR0000OFLA",
  "USC00352973",
  "USC00352997",
  "USR0000OFOR",
  "USC00353047",
  "USR0000OFOS",
  "USS0022G12S",
  "USR0000OGER",
  "USS0021G04S",
  "USC00353356",
  "USS0018E08S",
  "USC00353402",
  "USR0000OGAD",
  "USC00353445",
  "USR0000OGRM",
  "USS0021D01S",
  "USC00353542",
  "USC00353604",
  "USR0000OHAR",
  "USC00353692",
  "USR0000OHAY",
  "USC00353770",
  "USR0000OHEH",
  "USC00353827",
  "USC00353818",
  "USW00004113",
  "USR0000OHIG",
  "USS0018D19S",
  "USS0021E06S",
  "USS0022F42S",
  "USC00353995",
  "USC00354003",
  "USR0000OHOR",
  "USC00354060",
  "USR0000OHOY",
  "USR0000OILL",
  "USS0021F21S",
  "USR0000OJRI",
  "USW00004125",
  "USS0022E07S",
  "USR0000OKE2",
  "USR0000OKEL",
  "USC00354403",
  "USS0023G09S",
  "USR0000CKLA",
  "USC00354622",
  "USC00354606",
  "USR0000OLAG",
  "USS0018E18S",
  "USC00354721",
  "USC00354776",
  "USR0000OLAV",
  "USC00354811",
  "USC00354819",
  "USC00354835",
  "USR0000OLMC",
  "USS0022E09S",
  "USR0000OLOG",
  "USC00355050",
  "USC00355055",
  "USS0018D06S",
  "USS0019D03S",
  "USC00355160",
  "USC00355221",
  "USS0021E04S",
  "USC00355258",
  "USS0021E07S",
  "USW00094273",
  "USC00355392",
  "USW00024225",
  "USR0000OMER",
  "USR0000OMET",
  "USR0000OMID",
  "USS0017D20S",
  "USR0000OMLL",
  "USS0023D03S",
  "USC00355593",
  "USC00355638",
  "USC00355681",
  "USC00355711",
  "USR0000OMOO",
  "USR0000OMOR",
  "USS0017D06S",
  "USS0021D08S",
  "USS0017D18S",
  "USR0000OMOU",
  "USR0000OMTW",
  "USR0000OMTY",
  "USS0021D35S",
  "USR0000OMUT",
  "USC00355945",
  "USS0021F10S",
  "USW00024284",
  "USS0022D02S",
  "USR0000ONPR",
  "USC00356179",
  "USC00356213",
  "USS0020E02S",
  "USC00356252",
  "USW00024162",
  "USC00356366",
  "USC00356405",
  "USR0000OOWY",
  "USR0000OPHI",
  "USC00356426",
  "USR0000OPAR",
  "USR0000OPAT",
  "USS0021D14S",
  "USR0000OPEB",
  "USC00356532",
  "USW00024155",
  "USC00356550",
  "USC00356634",
  "USR0000OPOL",
  "USC00356784",
  "USW00094261",
  "USW00024229",
  "USC00356749",
  "USW00024242",
  "USC00356750",
  "USC00356820",
  "USC00356883",
  "USC00356907",
  "USR0000OPRO",
  "USR0000OQUA",
  "USS0020G06S",
  "USS0022F05S",
  "USR0000OREB",
  "USR0000ORED",
  "USS0021D04S",
  "USR0000OREM",
  "USW00024230",
  "USC00357127",
  "USR0000ORID",
  "USW00004128",
  "USS0022F43S",
  "USR0000OROB",
  "USR0000OROC",
  "USS0018F01S",
  "USR0000ORCK",
  "USC00357310",
  "USC00357331",
  "USR0000OROU",
  "USC00357391",
  "USR0000ORYE",
  "USS0023D01S",
  "USR0000OSAG",
  "USW00024232",
  "USS0022F04S",
  "USR0000OSAL",
  "USS0021E05S",
  "USW00004201",
  "USS0017D08S",
  "USC00357641",
  "USS0023D02S",
  "USR0000OSEL",
  "USC00357675",
  "USS0022G33S",
  "USW00024235",
  "USR0000OSIG",
  "USR0000OSIL",
  "USC00357809",
  "USS0021F12S",
  "USC00357817",
  "USC00357823",
  "USS0018G01S",
  "USC00357857",
  "USR0000OSLI",
  "USS0019F01S",
  "USS0022D03S",
  "USR0000OSFK",
  "USR0000OSPA",
  "USC00358029",
  "USR0000OSQU",
  "USS0019E07S",
  "USC00358095",
  "USR0000OSTR",
  "USS0020G09S",
  "USR0000OSUG",
  "USC00358173",
  "USS0020G02S",
  "USS0022F14S",
  "USR0000OSUM",
  "USS0021G17S",
  "USC00358246",
  "USS0021G16S",
  "USS0021G03S",
  "USS0017D07S",
  "USC00358407",
  "USS0021E13S",
  "USC00358466",
  "USC00358498",
  "USR0000OTIL",
  "USC00358494",
  "USR0000OTIM",
  "USS0018E09S",
  "USS0022F45S",
  "USC00358536",
  "USR0000OTOK",
  "USR0000OTRO",
  "USR0000OTUM",
  "USR0000OTUP",
  "USC00358726",
  "USR0000OUMA",
  "USC00358746",
  "USC00358884",
  "USR0000OVIL",
  "USR0000OWAG",
  "USC00358997",
  "USR0000OWAM",
  "USR0000OWAN",
  "USR0000OWAS",
  "USC00359316",
  "USR0000OWIL",
  "USC00359461",
  "USS0018D21S",
  "USC00359588",
  "USR0000OYLP",
  "USR0000OYEL",
  "USR0000OZIM"
)


OR_air_temp <- air_temp_stations %>%
  map(noaa_air, '2010-12-26', '2020-12-31') %>%
  bind_rows()

save(OR_air_temp, file = '//deqlab1/Assessment/Integrated_Report/DataSources/2022/NOAA air temp/NOAA_air_temp.RDATA')

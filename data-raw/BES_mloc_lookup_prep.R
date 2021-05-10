
# Monitoring location name lookup table ---------------------------------------------------------------------------

BES_mloc_lu <- read.csv('//deqlab1/Assessment/Integrated_Report/DataSources/2022/City of portland- continuous/Station_LookUp.csv')



usethis::use_data(BES_mloc_lu, overwrite = TRUE)

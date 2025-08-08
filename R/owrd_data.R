
#' Retrieve surface water monitoring results from Oregon Water Resources Department
#'
#' Data is retrieved from Oregon Water Resources Department's near real time monitoring results.
#' https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/
#'
#' @param station Required string vector of stations to be fetched.
#' @param startdate Required string setting the start date of the data being fetched. Format 'yyyy-mm-dd'.
#' @param enddate Required string setting the end date of the data being fetched. Format 'yyyy-mm-dd'.
#' @param char Required string vector identifying the characteristics to be fetched. Options include:
#'   * 'MDF' - Mean Daily Flow
#'   * 'Instantaneous_Stage' - Instantaneous Stage
#'   * 'Instantaneous_Flow' - Instantaneous Flow
#'   * 'Measurements' - Discharge grab measurements
#'   * 'WTEMP_MEASURE' - Water temperature grab measurements
#'   * 'WTEMP15' - Instantaneous Water temperature
#'   * 'WTEMP_MEAN' - Daily mean Water temperature
#'   * 'WTEMP_MAX' - Daily maximum Water temperature
#'   * 'WTEMP_MIN' - Daily minimum Water temperature
#' @export
#' @return data frame of data

owrd_data <- function(station, startdate, enddate, char) {

  # Testing
  # station=c("10378500", "13330000")
  # startdate="2020-12-01"
  # enddate="2021-02-15"
  # char=c("MDF", "WTEMP_MAX")

  WRDurl <- "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/hydro_download.aspx?"

  if(!any(char %in% c('WTEMP15', 'WTEMP_MEASURE', 'WTEMP_MEAN', 'WTEMP_MAX', 'WTEMP_MIN, MDF', 'Instantaneous_Stage', 'Instantaneous_Flow', 'Measurements'))) {
    stop("non valid value in 'char'")
  }

  df.query <- expand.grid(WRDurl=WRDurl, station=station, startdate=startdate, enddate=enddate, char=char)

  df.query <- df.query %>%
    dplyr::mutate(query=dplyr::case_when(char %in% c('MDF', 'Instantaneous_Stage', 'Instantaneous_Flow', 'Measurements') ~
                                           paste0(WRDurl,
                                                  "station_nbr=", station,
                                                  "&start_date=", startdate,
                                                  "%2012:00:00%20AM&",
                                                  "end_date=", enddate ,
                                                  "%2012:00:00%20AM&",
                                                  "dataset=", char,
                                                  "&format=tab"),
                                         TRUE ~
                                           paste0(WRDurl, "station_nbr=",station,
                                                  "&start_date=",startdate ,
                                                  "%2012:00:00%20AM&",
                                                  "end_date=",enddate ,
                                                  "%2012:00:00%20AM&",
                                                  "dataset=",char ,
                                                  "&format=tab&units=C")))


  df1 <- df.query %>%
    dplyr::group_by(query) %>%
    dplyr::group_split() %>%
    lapply(FUN = RCurl::getURL) %>%
    lapply(FUN = textConnection) %>%
    lapply(FUN=read.table, header = TRUE, sep ="\t", skip=0, stringsAsFactors=FALSE)

  # remove . from column names
  df2 <- lapply(df1, FUN=function(x){
    colnames(x) <- gsub(x=colnames(x), pattern="\\.", replacement="")
    return(x)})

  # pivot the data into long format
  df3 <- lapply(df2, tidyr::pivot_longer, cols=dplyr::any_of(c("mean_daily_flow_cfs", "instananteous_stage_ft", "instananteous_flow_cfs", "measured_stage_ft",
                                                               "temperature_measurement_C", "instantaneous_water_temp_C", "daily_mean_water_temp_C",
                                                               "daily_max_water_temp_C", "daily_min_water_temp_C",
                                                               "temperature_measurement_F", "instantaneous_water_temp_F", "daily_mean_water_temp_F",
                                                               "daily_max_water_temp_F", "daily_min_water_temp_F",
                                                               'daily_max_water_temp_CÂ')), values_to = "Result.Value", names_to = "Characteristic.Name")

  # fix various data types and add cols when they don't exist
  df4 <- df3 %>%
    lapply(FUN=dplyr::mutate, time_observed=if(!"time_observed" %in% colnames(.)) as.character(NA)) %>%
    lapply(FUN=dplyr::mutate, download_date=if(!"download_date" %in% colnames(.)) as.character(NA)) %>%
    lapply(FUN=dplyr::mutate, record_date=as.character(record_date)) %>%
    lapply(FUN=dplyr::mutate, published_status=as.character(published_status)) %>%
    lapply(FUN=dplyr::mutate, download_date=as.character(download_date)) %>%
    dplyr::bind_rows() %>%
    dplyr::select(station_nbr, record_date, time_observed, Characteristic.Name, Result.Value, published_status, download_date) #estimated, revised,

  return(df4)

}

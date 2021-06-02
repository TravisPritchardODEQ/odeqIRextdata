#' Retrieve historic climate data from NOAA
#'
#' Data is retrieved NOAA's NCEI Data service.
#' https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation
#'
#' @param station noaa air temperature station
#' @param startdate start date
#' @param enddate end date
#' @param data_type Data to return. Default is TMAX


noaa_air_tmax <- function(station, startdate, enddate, data_type = "TMAX"){

  base_url <- 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&units=standard&includeStationName=true&includeStationLocation=true&includeAttributes=true&format=json'

  type_url <- paste0(base_url, "&dataTypes=", data_type)

  date_url <- paste0(type_url, '&startDate=', startdate, '&endDate=', enddate)

  fetch_url <- paste0(date_url,'&stations=',station )

  df <- jsonlite::fromJSON(txt = fetch_url)

  return(df)
}

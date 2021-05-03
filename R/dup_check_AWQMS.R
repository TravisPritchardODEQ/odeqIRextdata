#' dup_check_AWQMS
#'
#' This function checks AWQMS for suspected duplicates when generating AWQMS import files. Function will query data
#' from AWQMS, join to provided datatable, and create a list of 3 objects: non_duplicates, suspected_dups,
#' suspected_updates. Suspected updates are matches in AWQMS, but with different results, suggesting there has been an
#' update somewhere (original dataset, data processing, etc..). This function will not currently work with continuous data.
#'
#' @param df Datafram of AWQMS import data to be checked against VW_AWQMS_Results in AWQMS
#' @param mloc_col Character of column name that holds MLocID information
#' @param date_col Character of column name that holds SampleStartDate information
#' @param time_col Character of column name that holds SampleStartTime information
#' @param char_col Character of column name that holds Char_Name information
#' @param stat_basis_col Character of column name that holds Statistical_Base information
#' @param depth_col Character of column name that holds act_depth_height information
#' @param method_col Character of column name that holds Method_Code information
#' @param result_col Character of column name that holds SampleStartDate information
#' @importFrom rlang .data




dup_check_AWQMS = function(df, mloc_col = "SiteID", date_col = 'ActStartDate',  time_col = "ActStartTime",
                           char_col = "CharID", stat_basis_col = "StatisticalBasis", depth_col = "SmplDepth",
                            method_col = "Method", result_col = "Result"){

# Testing ---------------------------------------------------------------------------------------------------------
#
# load(file = "NWIS_test_data.Rdata")
# df <- NWIS_sum_stats_data
# date_col <- 'ActStartDate'
# mloc_col <- "SiteID"
# char_col <- "CharID"
# time_col <- "ActStartTime"
# depth_col <- "SmplDepth"
# stat_basis_col <- "StatisticalBasis"
# method_col = "Method"
# result_col = "Result"



# Error checking --------------------------------------------------------------------------------------------------


if(DBI::dbCanConnect(odbc::odbc(), "AWQMS") == FALSE){

  stop("Unable to make database connection. Are you connected to VPN and have the appropriate ODBC connections?")
}
# Create dataframe of query inputs --------------------------------------------------------------------------------

query_params <- df %>%
  group_by(.data[[mloc_col]]) %>%
  summarise(min_date = min(.data[[date_col]]),
            max_date = max(.data[[date_col]]),
            chars = paste0("'", unique(.data[[char_col]]), "'", collapse = ", "))



# Iteratively build the query --------------------------------------------------------------------------------------

query <- "SELECT
OrganizationID,
MLocID,
Char_Name,
SampleStartDate,
cast([SampleStartTime] AS time(0)) as [SampleStartTime],
Method_Code,
act_depth_height,
Statistical_Base,
Result as AWQMS_Result,
Result_UID,
sample_fraction
  FROM [awqms].[dbo].[VW_AWQMS_Results]"

for(i in 1:nrow(query_params)) {

  if(i == 1){

  query <- query %>%
    paste0(" Where (MLocID = '",
                       query_params[i, 'SiteID'],
                       "' and SampleStartDate >= '",
                       as.Date(query_params[[i, 'min_date']]),
                       "' and Char_Name in (",query_params[[i, 'chars']] ,")) ")

  } else if(i > 1){

    query <- query %>%
      paste0(" OR (MLocID = '",
           query_params[i, 'SiteID'],
           "' and SampleStartDate >= '",
           as.Date(query_params[[i, 'min_date']]),
           "' and Char_Name in (",query_params[[i, 'chars']] ,")) ")

  }


}



# Query the AMQMS database ----------------------------------------------------------------------------------------

#Connect to database
con <- DBI::dbConnect(odbc::odbc(), "AWQMS")

# Create query language
qry <- glue::glue_sql(query, .con = con)


print("Begin AWQMS Query....")
# Query the database
data_fetch <- DBI::dbGetQuery(con, qry)

DBI::dbDisconnect(con)
print("End AWQMS Query....")


if(nrow(data_fetch) == 0){

  print("AWQMS Query provided no potential duplicates")

  dup_check_list <- list(non_duplicates = as.data.frame(df),
                         suspected_dups = as.data.frame(data_fetch),
                         suspected_updates = as.data.frame(data_fetch))

  return(dup_check_list)


}


# Basic manipulations so data matches import file -----------------------------------------------------------------


AWQMS_2_join <- data_fetch %>%
  select(-sample_fraction) %>%
  mutate(SampleStartDate = ymd(SampleStartDate),
         SampleStartTime = paste0(strsplit(SampleStartTime, ":")[[1]][1],
                                  ":",
                                  strsplit(SampleStartTime, ":")[[1]][2]) )



# Check for duplicates --------------------------------------------------------------------------------------------


dup_check <- df %>%
  mutate(AWQMS_start_date = ymd(.data[[date_col]]),
         AWQMS_start_time = ifelse(length(strsplit(.data[[time_col]], ":")[[1]][1]) == 1,
                                  paste0("0", strsplit(.data[[time_col]], ":")[[1]][1], ":", strsplit(.data[[time_col]], ":")[[1]][2]),
                                  .data[[time_col]]),
         AWQMS_depth = ifelse(.data[[depth_col]] == "", NA, .data[[depth_col]] ),
         AWQMS_Statistical_Base = case_when(.data[[stat_basis_col]] == 'Daily Maximum' ~ 'Maximum',
                                           .data[[stat_basis_col]] == "Daily Mean" ~ 'Mean',
                                           .data[[stat_basis_col]] == "Daily Minimum" ~ "Minimum",
                                           .data[[stat_basis_col]] == "7DMADMax" ~ "7DADM",
                                           .data[[stat_basis_col]] == "7DMADMean" ~ "7DADMean",
                                           .data[[stat_basis_col]] == "7DMADMin" ~ "7DADMin",
                                           .data[[stat_basis_col]] == "30DMADMean" ~ "30DADMean",
                                           TRUE ~ .data[[stat_basis_col]]),
         AWQMS_mloc = .data[[mloc_col]],
         AWQMS_CharID = .data[[char_col]],
         AWQMS_method = .data[[method_col]]
         ) %>%
  left_join(AWQMS_2_join, by = c('AWQMS_mloc' = 'MLocID',
                                 'AWQMS_CharID' = 'Char_Name',
                                 'AWQMS_start_date' = 'SampleStartDate',
                                 'AWQMS_start_time' = 'SampleStartTime',
                                 'AWQMS_method' = 'Method_Code',
                                 'AWQMS_depth' = 'act_depth_height',
                                 'AWQMS_Statistical_Base' = 'Statistical_Base'))



not_dup <- dup_check %>%
  filter(is.na(Result_UID)) %>%
  select(names(df))

suspected_dup <- dup_check %>%
  filter(!is.na(Result_UID),
         Result == AWQMS_Result)


suspected_update <- dup_check %>%
  filter(!is.na(Result_UID),
         .data[[result_col]] != AWQMS_Result)


# Return List -----------------------------------------------------------------------------------------------------

dup_check_list <- list(non_duplicates = as.data.frame(not_dup),
                       suspected_dups = as.data.frame(suspected_dup),
                       suspected_updates = as.data.frame(suspected_update))

return(dup_check_list)

}

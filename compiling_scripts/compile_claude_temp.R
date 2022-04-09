library(tidyverse)
library(lubridate)
library(tundra)

AF_database_dir <- "/media/ross/external/Copying_files/archive/duplicateCSV/AF_database/"
output_dir <- "/home/ross/Desktop/claude_temp/"

compile_claude_climate <- function(AF_database_dir, output_dir){
  if (!dir.exists(output_dir)){
    dir.create(output_dir, recursive = TRUE)
  }
  prev_compiled_files <- get_prev_compilations(AF_database_dir)
  new_data_files <- get_new_data_file_list(AF_database_dir)
  
  daily_data <- lapply(prev_compiled_files$daily, extract_data)
  daily_data <- plyr::rbind.fill(daily_data)
  
  hourly_data <- lapply(prev_compiled_files$hourly, extract_data)
  hourly_data <- plyr::rbind.fill(hourly_data)
  
  daily_new_data <- lapply(new_data_files$daily, extract_data)
  daily_data <- plyr::rbind.fill(daily_data, plyr::rbind.fill(daily_new_data))
  
  hourly_new_data <- lapply(new_data_files$hourly, extract_data)
  hourly_data <- plyr::rbind.fill(hourly_data, plyr::rbind.fill(hourly_new_data))
  
  # Between 2015-10-29 and 2016-06-12 something went wrong with the tower. Exclude this data
  
  daily_data <- daily_data %>% filter(date < "2015-10-29" | date > "2016-06-12")
  hourly_data <- hourly_data %>% filter(date < "2015-10-29" | date > "2016-06-12")
  
  # Create a site column and run standardise_plots
  
  daily_data <- daily_data %>% mutate(site = "Claude")
  daily_data <- standardise_plots(daily_data, output_dir, "")
  hourly_data <- hourly_data %>% mutate(site = "Claude")
  hourly_data <- standardise_plots(hourly_data, output_dir, "")
  
  # De-select columns that have been compiled already or are not understood and rearange columns
  # to bring all date columns to the fore
  
  cols_to_remove <- c("sensor", "wind_dir_d1_wvt", "bp_k_pa_avg", "record", "x1", "x2", "na")
  
  daily_data <- daily_data %>% select(!matches(".*snow(?!.*treatment)", perl = TRUE)) %>% 
    select(!any_of(cols_to_remove)) %>%
    relocate(c("site", "plot_id", "plot", "identifier", contains("treatment"), "date", "year", "day")) %>%
    arrange(date)
  hourly_data <- hourly_data %>% select(!matches(".*snow(?!.*treatment)", perl = TRUE)) %>% 
    select(!any_of(cols_to_remove)) %>%
    relocate(c("site", "plot_id", "plot", "identifier", contains("treatment"), "date", "year", "day", "hour")) %>%
    arrange(date)
  
  write_csv(daily_data, paste0(output_dir, "/claude_climate_data_1980-2018_daily.csv"))
  write_csv(hourly_data, paste0(output_dir, "/claude_climate_data_1980-2018_hourly.csv"))
  
  data <- list(daily = daily_data, hourly = hourly_data)
  
  
 

  data
}

get_prev_compilations <- function(AF_database_dir){
  
  data_dir <- paste0(AF_database_dir, "/Entered_data/Environmental_Data/Climate/Completed/")
  files <- list.files(data_dir, pattern = "claude|tower", full.names = TRUE, ignore.case = TRUE)
  daily_files <- files[grepl("daily", files, ignore.case = TRUE)]
  hourly_files <- files[grepl("hourly", files, ignore.case = TRUE)]
  file_list <- list(daily = daily_files, hourly = hourly_files)
  file_list
}

get_new_data_file_list <- function(AF_database_dir){
  new_data_file_list <- list(daily = c(
    paste0(AF_database_dir, 
           "AF_field_data_by_year/2018_climate_data/Tower/TOA5_ALEX_CR1000_1.Table2_2016_210_0000.dat")
  ),
  hourly = c(paste0(AF_database_dir, 
                    "AF_field_data_by_year/2018_climate_data/Tower/TOA5_ALEX_CR1000_1.Table1_2016_209_1000.dat")
  )
  )
  new_data_file_list
}


extract_data <- function(file_path){
  print(file_path)
  timestamp_files <- c("Climate.Claude_Tower_ALLYEARS_(80-16)_2012_2016_Daily.csv",
                       "TOA5_ALEX_CR1000_1.Table2_2016_210_0000.dat",
                       "TOA5_ALEX_CR1000_1.Table1_2016_209_1000.dat",
                       "Climate.Claude_Tower_ALLYEARS_(80-16)_2012_2016_Hourly.csv")
  non_date_timestamp_files <- c("Climate.Claude_Tower_ALLYEARS_(80-16)_2012_2016_Hourly.csv",
                                "Climate.Claude_Tower_ALLYEARS_(80-16)_2012_2016_Daily.csv")
  timestamp = FALSE
  for (timestamp_file in timestamp_files){
    
    if (grepl(timestamp_file, file_path, fixed = TRUE)){
      timestamp = TRUE
    }
  }
  
  non_date_timestamp = FALSE
  for (non_date_timestamp_file in non_date_timestamp_files){
    
    if (grepl(non_date_timestamp_file, file_path, fixed = TRUE)){
      non_date_timestamp = TRUE
    }
  }
  
  
  if (timestamp){
    skip <- 0
    found_header <- FALSE
    while (!found_header){
      dataframe <- load_file(file_path, skip = skip)
      if ("timestamp" %in% names(dataframe)){
        found_header <- TRUE
      } else {
        skip = skip + 1
      }
      if (skip == 5){
        stop("no header line found for timestamp file")
      }
    }
    if (non_date_timestamp){
      dataframe <- dataframe %>% mutate(timestamp = as.numeric(timestamp),
                                        timestamp = as_date(timestamp, origin = "1899-12-30") +
                                          seconds_to_period((timestamp - trunc(timestamp))*24*60*60)
      )
      
    } else {
      dataframe <- dataframe %>% mutate(timestamp = as_date(timestamp))
    }
    if (sum(hour(dataframe$timestamp), na.rm = TRUE) > 0){
      dataframe <- dataframe %>% mutate(hour = as.character(hour(timestamp)),
                                        minute = as.character(minute(timestamp)),
                                        hour = ifelse(minute == "59",
                                                      ifelse(hour != 23,
                                                             as.character(as.numeric(hour) + 1),
                                                             "00"),
                                                      hour),
                                        minute = ifelse(minute == "59", "00", minute),
                                        hour = ifelse(nchar(hour) == 2, hour, paste0("0", hour)),
                                        minute = ifelse(nchar(minute) == 2, minute, paste0("0", minute)),
                                        
                                        hour = paste0(hour, minute),
                                        hour = ifelse(is.na(as.numeric(hour)), NA, hour)
      ) %>% select(!minute)
      
    }
    
    
    dataframe <- dataframe %>% mutate(year = year(timestamp),
                                      day = yday(timestamp)) %>%
      select(!timestamp)
  } else {
    dataframe <- load_file(file_path)
  }
  dataframe <- dataframe %>% mutate(file = file_path)
  dataframe <- dataframe %>% relocate(any_of(c("year", "day", "hour")))
  dataframe <- add_date(dataframe)
  dataframe <- standardise_cols(dataframe)
  
  if (!"hour" %in% names(dataframe) & "time" %in% names(dataframe)){
    dataframe <- dataframe %>% rename(hour = time)
  }
  dataframe
}

create_syn_list <- function(){
  syn_lists <- list(
    temp_air_min = c("temp_air_2m_min", "airTC_Min", "minair_t", "air_tc_min"),
    temp_air_max = c("temp_air_2m_max", "AirTC_Max", "maxair_t", "air_tc_max"),
    temp_air_mean = c("temp_air_2m_mean", "AirTC_Avg", "avair_t", "air_tc_avg"),
    temp_surface_min = c("temp_surface_-2cm_min", "minsurf_t"),
    temp_surface_max = c("temp_surface_-2cm_max", "mxsurf_t"),
    temp_surface_mean = c("temp_surface_-2cm_mean", "avsurf_t"),
    temp_10cm_mean = c("temp_soil_-10cm_mean", "av10cm_t"),
    temp_10cm_min = c("temp_soil_-10cm_min", "min10cm_t"),
    temp_10cm_max = c("temp_soil_-10cm_max", "mx10cm_t"),
    temp_25cm_mean = c("temp_soil_-25cm_mean", "av25cm_t"),
    temp_25cm_min = c("temp_soil_-25cm_min", "min25cm_t"),
    temp_25cm_max = c("temp_soil_-25cm_max", "mx25cm_t"),
    relative_humidity_mean = c("relative_humidity_mean", "RH", "rh_percent", "rh_avg"),
    relative_humidity_min = c("relative_humidity_min", "RH_Min", "rh_percent_min"),
    relative_humidity_max = c("relative_humidity_max", "RH_Max", "rh_percent_max"),
    wind_speed_mean = c("wind_speed_mean_km/h", "avwind", "WS_kph_S_WVT"),
    wind_speed_max = c("wind_speed_max_km/h", "wind_max"),
    wind_direction_mean = c("wind_direction_mean", "directn"),
    wind_direction_max = c("wind_direction_max", "mxwinddir"),
    wind_direction_SD = c("wind_direction_standard_deviation", "sddirec",
                          "WindDir_SD1_WVT", "wind_dir_sd1_wvt"),
    snow_depth = c("snow_depth_(mm_to_ground)", "TCDT_Avg"),
    Solar_radiation = c("solar_radiation_mean_W", "SlrW_Avg", "slr_w_avg", "k_wm2", "kwm2"),
    snow_surface_temp_mean = c("snow_surface_temp_mean", "TargTempC_Avg", "targ_temp_c_avg"),
    snow_surface_temp_max = c("snow_surface_temp_max", "TargTempC_Max", "targ_temp_c_max"),
    snow_surface_temp_min = c("snow_surface_temp_min", "TargTempC_Min", "targ_temp_c_min"),
    time_of_min_temp_air = c("time_of_min_temp_air", "time_airmin_t"),
    time_of_max_temp_air = c("time_of_max_temp_air", "time_airmax_t", "time_airmx_t")
    # unknown = "windDir_D1_WVT",
  )
}

standardise_cols <- function(dataframe){
  syn_lists <- create_syn_list()
  for (syn_list in syn_lists){
    for (i in 1:length(names(dataframe))){
      if (tolower(names(dataframe)[i]) %in% tolower(syn_list)){
        names(dataframe)[i] <- syn_list[1]
      }
    }
  }
  dataframe <- dataframe %>% select(!starts_with("NA_._"))
  dataframe
}


#!/bin/env Rscript

library(tidyverse)
library(lubridate)
library(hablar)
library(tundra)

#' A function which loads a file of a given file_path and checks for the presence of an
#' Identifier column. If it finds one then it isolates the data associated with each
#' Identifier and attempts to locate columns containing Day, Year, Hour, and Solar radiation
#' data. If no day, year, or solar data is identified then the function moves on to the next
#' identifier. If these critical columns are detected then a CSV file is written in the output_dir
#' containing as many of the following as are present
#' Identifier
#' Date - determined using the add_date function
#' Year
#' Day
#' fractional day - A combination of day and hour e.g. 12.5 = noon on the 12th of Jan
#' Hour
#' Solar
#' file - The file_path to the original data
#' 
#' Files are found using the file_finder.sh bash script. Each site can be searched for using
#' the following regex:
#' Cassiope: cass(?!andra)
#' Claude: clau|tower
#' meadow: mead
#' Fosheim: fosh
#' Marie_bay: (?<!sum)(marie|bay)
#' @param file_path A file path to a CSV or tab deliminated file that is to be searched for
#' solar radiation data
#' @param output_dir The directory to which all output shall be written
#' @param W_or_KW Shall data be searched for in Watts or Kilowatts. A string - either "W" or "KW"
#' @param name A string that will be augmented with a year or day range to provide the file names
#' for all of the produced csv files
#' @param split_file Shall the load_file function, if it encounters rows of different lengths return
#' separate dataframes for each row length. This is ideal for separating out the climate data created
#' by the climate stations and makes the data from such files more easily interpratable

Colate_claude_solar <- function(file_path, output_dir, W_or_KW = "KW", name, compiled_site, split_file = TRUE){
  
  print(file_path) #Allows the monitoring of the scripts progress while running
  
  # Some files contain characters which R can not read. If this file contains
  # such characters, make a note of it and skip to next file. Files with no data
  # will also crash R.
  if (file.info(file_path)$size == 0){
    return()
  }
  encoding <- guess_encoding(file_path)
  if (length(encoding[[1]]) == 0){
    write_lines(file_path, paste0(output_dir, "/cant_read.txt"), append = TRUE)
    return()
  }
  
  # Load the data
  dataframe <- load_file(file_path, split_file = split_file)
  
  if (is.data.frame(dataframe)){
    extract_solar(dataframe, file_path, output_dir, W_or_KW, name, compiled_site)
  } else {
    lapply(seq_along(dataframe), FUN = function(dataframe,
                                                file_path,
                                                output_dir,
                                                W_or_KW,
                                                name,
                                                i,
                                                compiled_site){
      extract_solar(dataframe[[i]], file_path, output_dir, W_or_KW, name, compiled_site)
    },
    dataframe = dataframe,
    file_path = file_path,
    output_dir = output_dir,
    W_or_KW = W_or_KW,
    name = name,
    compiled_site = compiled_site)
  }
}

extract_solar <- function(dataframe, file_path, output_dir, W_or_KW = "KW", name, compiled_site){
  # Add site name to the dataframe
  dataframe <- mutate(dataframe, site = compiled_site)
  # Check if the file contains a column consistent with an identifier column
  # If not, skip to next file. is_identifier is a logical vector with a True/False
  # value for every column in the dataframe
  is_identefier <- identifier_without_target(dataframe, output_dir, file_path)
  if (sum(is_identefier) > 0){
    names(dataframe)[is_identefier] <- "Identifier"
    dataframe <- itterate_duplicated_col_names(dataframe, "Identifier")
  }
  
  # Create list of unique identifiers in identifier column. If no identifier column
  # was detected determine_identifiers will return NULL
  identifiers <- determine_identifiers(dataframe, is_identefier)
  
  # If an identifier column was found then sequentially check with each identifier for the
  # appropriate data and produce a compiled file (for each identifier if necessary). If
  # no identifier column was identified, check to see if data can be found with no identifier.
  if (!is.null(identifiers)){
    lapply(identifiers, FUN = function(dataframe,
                                       is_identefier,
                                       output_dir,
                                       file_path,
                                       W_or_KW,
                                       name,
                                       identifier){
      extract_rest_of_columns(identifier,
                              dataframe,
                              is_identefier,
                              output_dir,
                              file_path,
                              W_or_KW,
                              name)
    },
    dataframe = dataframe,
    is_identefier = is_identefier,
    output_dir = output_dir,
    file_path = file_path,
    W_or_KW = W_or_KW,
    name = name)
    
  } else {
    extract_rest_of_columns(NULL, dataframe, is_identefier, output_dir, file_path, W_or_KW, name)
  }
}

extract_rest_of_columns <- function(identifier,
                                    dataframe,
                                    is_identefier,
                                    output_dir,
                                    file_path,
                                    W_or_KW,
                                    name){
  # Remove all data not pertaining to Identifier target
  if (!is.null(identifier)){
    dataframe <- filter(dataframe, grepl(paste0('(^|,)"? *', identifier, ' *"?($|,)'),
                                         dataframe[is_identefier][[1]]))
  }
  
  # Day identifier returns a logical vector with a value for every column in dataframe
  # The value will be TRUE if that column's data is consistent with Days (1-366).
  is_day <- day_identifier(dataframe)
  if (length(is_day) == 0 || sum(is_day) != 1){
    return()
  }
  names(dataframe)[is_day] <- "Day"
  dataframe <- itterate_duplicated_col_names(dataframe, "Day")
  
  # Year identifier returns a logical vector with a value for every column in dataframe
  # The value will be TRUE if that column's data is consistent with years (1980-2030).
  is_year <- year_identifier(dataframe)
  if (length(is_year) == 0 | sum(is_year) == 0){
    write_lines(paste0("Identifier: ", identifier, "path: ", file_path),
                paste0(output_dir, "/missing_year_needs_investigating.txt"),
                sep = "\n", append = TRUE)
    return()
  }
  # name column and ensure column names are not duplicated
  names(dataframe)[is_year] <- "Year"
  dataframe <- itterate_duplicated_col_names(dataframe, "Year")
  
  # Add a date column to the dataframe and update is_day and is_year to include the new column
  dataframe <- add_date(dataframe, is_day, is_year)
  is_day <- append(is_day, FALSE)
  is_year <- append(is_year, FALSE)
  
  is_hour <- hour_identifier(dataframe, is_day, is_year)
  if (sum(is_hour[[1]]) > 0){
    dataframe <- is_hour[[2]]
    is_hour <- is_hour[[1]]
    is_day <- append(is_day, FALSE)
    is_year <- append(is_year, FALSE)
    # is_hour <- append(is_hour, FALSE)
    names(dataframe)[is_hour] <- "Hour"
    dataframe <- itterate_duplicated_col_names(dataframe, "Hour")
    
    
    is_solar <- solar_identifier(dataframe, file_path, output_dir, W_or_KW, hourly_data = TRUE)
    
  } else if (length(unique(dataframe$date)) > length(dataframe$date)/2){
    
    is_hour <- is_hour[[1]]
    is_solar <- solar_identifier(dataframe, file_path, output_dir, W_or_KW, hourly_data = FALSE)
    
  } else {
    readr::write_lines(file_path,
                       file = paste0(output_dir, "/missing_hour.txt"),
                       sep = "\n",
                       append = TRUE)
    return()
  }
  
  if (length(is_solar) == 0 | sum(is_solar) == 0){
    return()
  }
  names(dataframe)[is_solar] <- "Solar"
  dataframe <- itterate_duplicated_col_names(dataframe, "Solar")
  
  dataframe <- mutate(dataframe, file = file_path)
  
  solar_dataframe <- dplyr::select(dataframe, dplyr::any_of(c(
    dplyr::matches("^site(_[0-9])?$"),
    dplyr::matches("^Identifier(_[0-9])?$"),
    dplyr::matches("^date(_[0-9])?$"),
    dplyr::matches("^Year(_[0-9])?$"),
    dplyr::matches("^Day(_[0-9])?$"),
    dplyr::matches("fractional_day"),
    dplyr::matches("^Hour(_[0-9])?$"),
    dplyr::matches("^Solar(_[0-9])?$"),
    dplyr::matches("^file(_[0-9])?$"))))
  
  
  # Create a new file in the output directory containing the solar_dataframe
  if (sum(is_hour) == 0){
    save_dataframe(solar_dataframe, output_dir, name)
  } else {
    if (!dir.exists(paste0(output_dir, "/hourly"))){
      dir.create(paste0(output_dir, "/hourly"))
    }
    save_dataframe(solar_dataframe, paste0(output_dir, "/hourly"), name)
  }
}



# A function which accepts a dataframe, and the logical vectors is_day, is_year,
# is_identifier, is_snow, and is_site, and uses these logical vectors to produce
# a new dataframe containing only Identifier, Year, Day, Snow_depth, and site data.
# Finally, it creates a new column, file, detailing the file path to the original data.
construct_solar_data <- function(dataframe, is_day, is_year,  is_identifier,
                                 is_solar, file_path){
  
  if (sum(is_year) > 0){
    solar_dataframe <- dataframe %>% select(colnames(dataframe)[is_identifier],
                                            date,
                                            colnames(dataframe)[is_year],
                                            colnames(dataframe)[is_day],
                                            colnames(dataframe)[is_solar]) %>%
      # Create new column, file, detailing where the original data was found
      mutate(file = file_path)
  } else {
    solar_dataframe <- dataframe %>% select(colnames(dataframe)[is_identifier],
                                            date,
                                            colnames(dataframe)[is_day],
                                            colnames(dataframe)[is_solar]) %>%
      # Create new column, file, detailing where the original data was found
      mutate(file = file_path)
  }
  
  solar_dataframe
}

Colate_solar_from_bash <- function(){
  arguments <- commandArgs(TRUE)
  number_of_files <- as.numeric(arguments[1])
  output_dir <- arguments[2]
  W_or_KW <- arguments[3]
  name <- arguments[4]
  split_file <- as.logical(arguments[5])
  site <- arguments[6]
  
  print(arguments)
  print(split_file)
  
  # number_of_files <- 13623
  # output_dir <- "/home/ross/Desktop/cassiope/"
  # W_or_KW <- "KW"
  # name <- "Cassiope_Solar"
  # split_file <- TRUE
  # site <- "cassiope"
  
  if (!file.exists(paste0(output_dir, "/index.txt"))){
    i <- 0
    write_lines(i, paste0(output_dir, "/index.txt"))
  }
  
  start_index <- as.numeric(read_lines(paste0(output_dir, "/index.txt")))
  
  if (start_index < number_of_files){
    for (i in start_index:(number_of_files - 1)){
      file_path <- readr::read_lines(paste0(output_dir, "/files.txt"),
                                     skip = i,
                                     n_max = 1)
      
      
      Colate_claude_solar(file_path, output_dir, W_or_KW, name, site, split_file)
      i <- i + 1
      write_lines(i, paste0(output_dir, "/index.txt"))
    }
  }
  
  if (W_or_KW == "W"){
    W_or_KW <- "KW"
    first_unit <- "W"
  } else {
    W_or_KW <- "W"
    first_unit <- "KW"
  }
  new_name <- paste0(name, "_", W_or_KW)
  new_output_dir <- paste0(output_dir, "/", new_name)
  if (!dir.exists(new_output_dir)){
    dir.create(new_output_dir)
  }
  if (!file.exists(paste0(new_output_dir, "/files.txt"))){
    file.copy(paste0(output_dir, "/files.txt"),
              paste0(new_output_dir, "/files.txt"))
  }
  if (!file.exists(paste0(new_output_dir, "/index.txt"))){
    i <- 0
    write_lines(i, paste0(new_output_dir, "/index.txt"))
  }
  start_index <- as.numeric(read_lines(paste0(new_output_dir, "/index.txt")))
  for (i in start_index:(number_of_files - 1)){
    file_path <- readr::read_lines(paste0(new_output_dir, "/files.txt"),
                                   skip = i,
                                   n_max = 1)
    
    
    Colate_claude_solar(file_path, new_output_dir, W_or_KW, new_name, site, split_file)
    i <- i + 1
    write_lines(i, paste0(new_output_dir, "/index.txt"))
  }
  consolidate_units(output_dir, first_unit = first_unit, name = name)
}

consolidate_units <- function(file_path, first_unit, name){
  
  if (first_unit == "W"){
    second_unit <- "KW"
    conversion_factor <- 1000
  } else if (first_unit == "KW"){
    second_unit <- "W"
    conversion_factor <- 0.001
  } else {
    stop("invalid first_unit")
  }
  path_to_second_daily <- paste0(file_path, "/", name, "_", second_unit, "/")
  path_to_second_hourly <- paste0(path_to_second_daily, "/hourly")
  
  if (dir.exists(path_to_second_daily)){
    convert_units(file_path, 
                  path_to_second_daily,
                  conversion_factor,
                  first_unit,
                  second_unit,
                  name,
                  hourly = FALSE
                  )
  }
  if (dir.exists(path_to_second_hourly)){
    convert_units(file_path, 
                  path_to_second_hourly,
                  conversion_factor,
                  first_unit,
                  second_unit,
                  name,
                  hourly = TRUE
    )
  }
  print("done!")
  
}

convert_units <- function(file_path, dir_to_convert, conversion_factor, first_unit, second_unit, name, hourly){
  files_to_convert <- list.files(path = dir_to_convert, 
                                 pattern = ".csv", 
                                 full.names = TRUE,
                                 recursive = FALSE,
                                 ignore.case = TRUE, 
                                 include.dirs = FALSE
  )
  if (length(files_to_convert) > 0){
    if (hourly){
      file_path <- paste0(file_path, "/hourly/")
    }
    lapply(files_to_convert, perform_unit_conversion,
           output_dir = file_path,
           conversion_factor = conversion_factor, 
           first_unit = first_unit,
           second_unit = second_unit
           )
  }
}

perform_unit_conversion <- function(file_path, conversion_factor, output_dir, first_unit, second_unit){
  conversion_factor <- conversion_factor
  dataframe <- load_file(file_path)
  file_name <- sub(".*/", "", file_path)
  file_name <- sub(paste0("_", second_unit, "_.*"), paste0("_", first_unit), file_name)
  
  dataframe <- dataframe %>% ungroup() %>%
    mutate(across(starts_with("Solar"), .fns = as.numeric),
           across(starts_with("Solar"), .fns = ~ .x * {conversion_factor}),
           converted := TRUE)
  save_dataframe(dataframe, output_dir, name = paste0(file_name, "_converted_from_", second_unit))
  
  
}
Colate_solar_from_bash()


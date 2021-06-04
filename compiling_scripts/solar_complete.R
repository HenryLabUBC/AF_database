#!/bin/env Rscript

library(tidyverse)
library(lubridate)
library(hablar)
library(tundra)


Colate_claude_solar <- function(file_path, output_dir, W_or_KW = "KW", name, split_file = TRUE){

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
    
    if (is.list(dataframe)){
      lapply(seq_along(dataframe), FUN = function(dataframe,
                                                  file_path,
                                                  output_dir,
                                                  W_or_KW,
                                                  name,
                                                  i){
        extract_solar(dataframe[[i]], file_path, output_dir, W_or_KW, name)
        },
        dataframe = dataframe,
        file_path = file_path,
        output_dir = output_dir,
        W_or_KW = W_or_KW,
        name = name)
    } else {
    extract_solar(dataframe, file_path, output_dir, W_or_KW, name)
}
}

extract_solar <- function(dataframe, file_path, output_dir, W_or_KW = "KW", name){
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
  if (length(is_day) == 0 || sum(is_day) == 0){
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
    is_hour <- append(is_hour, FALSE)
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
    dplyr::matches("^Identifier(_[0-9])?$",),
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

print(arguments)

number_of_files <- 326
output_dir <- "/media/ross/external/Copying_files/archive/claude_complete/"
W_or_KW <- "W"
name <- "claude_solar"
split_file <- TRUE

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


Colate_claude_solar(file_path, output_dir, W_or_KW, name, split_file)
i <- i + 1
write_lines(i, paste0(output_dir, "/index.txt"))
}
}

if (W_or_KW == "W"){
  W_or_KW <- "KW"
} else {
  W_or_KW <- "W"
}
name <- paste0(name, "_", W_or_KW)
new_output_dir <- paste0(output_dir, "/", name)
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
  
  
  Colate_claude_solar(file_path, new_output_dir, W_or_KW, name, split_file)
  i <- i + 1
  write_lines(i, paste0(new_output_dir, "/index.txt"))
}
}
Colate_solar_from_bash()


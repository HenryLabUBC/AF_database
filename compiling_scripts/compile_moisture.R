library(tidyverse)
library(tundra)
library(parallel)

extract_moisture <- function(file_path, output_dir){
  # Load the next soil moisture file. 
  dataframe <- load_file(file_path)
  # If a list of dataframes was returned by load_file, combine into one dataframe
  if (!is.data.frame(dataframe)){
    dataframe <- do.call(plyr::rbind.fill, dataframe)
  }
  
  # Skip file if it is the 93-09 compiled data, as it is duplicated and unclear
  if (grepl("all_sites_93-09|93-2011", file_path) & "wet_bag" %in% names(dataframe)){
    return()
  }
  # Identify columns which had duplicated names in the original data file. load_file automatically
  # adds an iterator to duplicated column names (but not the first instance) separated by "_._"
  duplicate_cols <- grepl("_\\._[0-9]$", names(dataframe))
  # If duplicates were found then the data is in a "wide" form with the same columns present twice.
  # convert to a "long" form.
  if (sum(duplicate_cols) > 0){
    # isolate left and right dataframes
    dataframe_1 <- dataframe %>% select(!matches("_\\._[0-9]$"))
    dataframe_2 <- dataframe %>% select(matches("_\\._[0-9]$"))
    # remove the iterator from column names in right hand dataframe
    names(dataframe_2) <- str_extract(names(dataframe_2), ".*(?=_\\._[0-9]$)")
    
    # combine left and right dataframes in "long" form
    dataframe <- plyr::rbind.fill(dataframe_1, dataframe_2)
  }
  # Standardise all column names
  dataframe <- standardise_names(dataframe)
  
  if ("n" %in% names(dataframe)){
    print("in")
  }
  
  # add a column containing the file path
  dataframe <- mutate(dataframe, file = file_path)
  
  # The position the measurement was taken within the plot (north, south, centre), and sometimes
  # the angle the probe was inserted (15, 90), is contained within column names in wide form. Convert
  # to long form. If no position columns are found then "no position" is returned
  dataframe_with_position <- format_positions(dataframe, file_path, output_dir)
  # If no position is found then skip to next file
  if (!is.data.frame(dataframe_with_position)){
    if (sum(grepl("moisture", names(dataframe), ignore.case = TRUE)) > 0){
      dataframe <- mutate(dataframe, position = NA)
      names(dataframe)[grepl("moisture", names(dataframe))] <- "soil_moisture"
    } else {return()}
  } else {
    dataframe <- dataframe_with_position
  }
  
  # If no site column is present try to extract site data from the file meta-data or file_path
  if (!"site" %in% names(dataframe)){
    dataframe <- fill_parameter(dataframe, site_extractor, output_dir, file_path, "site", breaks = FALSE)
  }
  # Extract the year from the file or file path and add to a column named "year"
  dataframe <- fill_parameter(dataframe, year_extractor, output_dir, file_path, "year")
  # Extract the day from the file or file path and add to a column named "day". Search for numbers
  # from 100-366
  DOY_names <- c(',"DOY', ',"D.O.Y', ',"day.of.year', ',"day', ',"date')
  dataframe <- fill_parameter(dataframe,
                              DOY_extractor,
                              output_dir,
                              file_path,
                              "day",
                              breaks = TRUE,
                              DOY_names,
                              three_digits = TRUE)
  # Determine if the day was successfully extracted
  DOY_present <- parameter_present(dataframe, DOY_extractor, col_name = "day",
                                   three_digits = TRUE, breaks = TRUE)
  
  # If the day of year was not found, try again, looking for a calendar date
  if (!DOY_present){
    dataframe <- fill_parameter(dataframe,
                                date_extractor,
                                output_dir,
                                file_path,
                                breaks = TRUE,
                                "day",
                                date_day_month = "date")
    # Check if a date was successfully extracted
    date_present <- parameter_present(dataframe, date_extractor, breaks = TRUE,
                                      col_name = "day", date_day_month = "date")
    # If a date was found, convert it to a DOY
    if (date_present){
      # Extract the dates found by fill_parameter
      date <- date_extractor(dataframe$day)
      # Extract the days from those dates
      day <- date_extractor(date, date_day_month = "day")
      # If one, and only one day is found, determine the DOY and add it to the column "day"
      if (length(day) == 1){
        # Extract month and year from date
        year <- year_extractor(date)
        month <- date_extractor(date, date_day_month = "month")
        # Combine into a posix date format
        date <- lubridate::ymd(paste0(year, month, day))
        # Determine day of year
        DOY <- lubridate::yday(date)
        # Add DOY to "day" column
        dataframe <- mutate(dataframe, day = DOY)
        # Remove the entry in missing_day.txt that was produced when searching for the DOY
        # and not the date
        missing_day_files <- read_lines(paste0(output_dir, "/missing_day.txt"))
        missing_day_files <- gsub(file_path, "", missing_day_files, fixed = TRUE)
        write_lines(missing_day_files, paste0(output_dir, "/missing_day.txt"))
      }
    }
  }
  
  # new_dataframe <- "crashed"
  # try(new_dataframe <- standardise_plots(dataframe, output_dir, file_path))
  # 
  # if (is.data.frame(new_dataframe)){
  #   dataframe <- new_dataframe
  # } else {
  #   standardise_plots(dataframe, output_dir, file_path)
  # }
  # Standardise all plot information
  dataframe <- standardise_plots(dataframe, output_dir, file_path)
  
  
  
  dataframe
}
compile_moisture <- function(){
  output_dir <- "/media/ross/external/Copying_files/archive/moisture/"
  file_paths <- read_lines("/media/ross/external/Copying_files/archive/moisture/files.txt")
  files <- lapply(file_paths, extract_moisture, output_dir = output_dir)
  dataframe <- do.call(plyr::rbind.fill, files)
  dataframe
}




standardise_names <- function(dataframe){
  for (name in names(dataframe)){
    otc_names <- c("otc_cntr", "otc", "otc_cntrl", "treatment", "treat")
    comment_names <- c("comment", "comments", "notes")
    day_names <- c("day", "day_of_year", "date")
    condition_names <- c("condition", "conditions")
    
    if (name %in% otc_names){
      names(dataframe)[names(dataframe) == name] <- "treatment"
    }
    if (name %in% comment_names){
      names(dataframe)[names(dataframe) == name] <- "comments"
    }
    if (name %in% day_names){
      names(dataframe)[names(dataframe) == name] <- "day"
    }
    if (name %in% condition_names){
      names(dataframe)[names(dataframe) == name] <- "conditions"
    }
    
  }
  names(dataframe) <- gsub("center(.*)", "centre\\1", names(dataframe))
  dataframe
}

format_positions <- function(dataframe, file_path, output_dir){
  positions <- c("north", "south", "east", "west", "centre", "north_90", "north_15",
                 "south_90", "south_15", "centre_90", "centre_15", "n", "e", "s", "w", "c")
  position_present <- sum(positions %in% names(dataframe)) > 1
  if (position_present){
    dataframe <- dataframe %>% pivot_longer(any_of(positions),
                                            values_to = "soil_moisture",
                                            names_to = c("position", "angle"),
                                            names_sep = "_")
    data_missing <- sum(is.na(dataframe$soil_moisture) |
                          is.null(dataframe$soil_moisture))/length(dataframe$soil_moisture) > 0.9
    if (data_missing){
      return("data missing")
    }
  } else {
    write_lines(file_path, append = TRUE, file = paste0(output_dir, "/missing_position.txt"))
    return("no position")
  }
  dataframe
}
find_no_positions <- function(file_path, output_dir){
  dataframe <- load_file(file_path, multi_header = TRUE)
  # If a list of dataframes was returned by load_file, combine into one dataframe
  if (!is.data.frame(dataframe)){
    dataframe <- do.call(plyr::rbind.fill, dataframe)
  }
  # Identify columns which had duplicated names in the original data file. load_file automatically
  # adds an iterator to duplicated column names (but not the first instance) separated by "_._"
  duplicate_cols <- grepl("_\\._[0-9]$", names(dataframe))
  # If duplicates were found then the data is in a "wide" form with the same columns present twice.
  # convert to a "long" form.
  if (sum(duplicate_cols) > 0){
    # isolate left and right dataframes
    dataframe_1 <- dataframe %>% select(!matches("_\\._[0-9]$"))
    dataframe_2 <- dataframe %>% select(matches("_\\._[0-9]$"))
    # remove the iterator from column names in right hand dataframe
    names(dataframe_2) <- str_extract(names(dataframe_2), ".*(?=_\\._[0-9]$)")
    
    # combine left and right dataframes in "long" form
    dataframe <- plyr::rbind.fill(dataframe_1, dataframe_2)
  }
  # Standardise all column names
  dataframe <- standardise_names(dataframe)
  
  # add a column containing the file path
  dataframe <- mutate(dataframe, file = file_path)
  
  # The position the measurement was taken within the plot (north, south, centre), and sometimes
  # the angle the probe was inserted (15, 90), is contained within column names in wide form. Convert
  # to long form. If no position columns are found then "no position" is returned
  dataframe_with_position <- format_positions(dataframe, file_path, output_dir)
  # If no position is found then skip to next file
  if (!is.data.frame(dataframe_with_position) & dataframe_with_position == "no position"){
    if (sum(grepl("weight|mass|bag", names(dataframe), ignore.case = TRUE)) > 0){
      return()
    } else {return(dataframe)}
  } else {
    return()
  }
  return()
}

compile_no_position <- function(){
  output_dir <- "/media/ross/external/Copying_files/archive/moisture/"
  file_paths <- read_lines("/media/ross/external/Copying_files/archive/moisture/files.txt")
  files <- lapply(file_paths, find_no_positions, output_dir = output_dir)
  dataframe <- do.call(plyr::rbind.fill, files)
  dataframe
}
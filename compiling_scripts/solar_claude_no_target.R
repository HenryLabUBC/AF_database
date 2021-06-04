#!/bin/env Rscript

library(tidyverse)
library(lubridate)
library(hablar)


Colate_claude_solar <- function(target){
  # The directory that shall be recursively searched for snowdepth data
  file_dir = "/media/ross/external/Copying_files/archive/duplicateCSV/"
  # The directory in which all results shall be stored
  output_dir = "/media/ross/external/Copying_files/archive/claude_solar/"
  if (!dir.exists(output_dir)){dir.create(output_dir)}

  # A regex expression used to filter file and path names
  pattern <- "clau|tower"

  #call the function to create a file containing a list of all CSV files that have
  #at least one cell matching the target

  if (!file.exists(paste0(output_dir, "files.txt"))){
    find_files_no_identifier(file_dir, output_dir)
  }
  #read the first file path contained in the file created by find_files
  file_path <- read_lines(paste0(output_dir, "/files.txt"), n_max = 1)
  # While there is still files left to check in files.txt, attempt to identify columns
  # containing Identifiers, Years, Days, and Snow Depth Data. If appropriate data is
  # found, save it to a new file in the output directory before moving on to the next
  # file in files.txt
  while (!is.na(file_path)){
    print(file_path) #Allows the monitoring of the scripts progress while running

    if (!grepl("clau|tower", file_path, ignore.case = TRUE)){
      file_path <- next_file(output_dir)
      next}

    # Some files contain characters which R can not read. If this file contains
    # such characters, make a note of it and skip to next file.
    if (file.info(file_path)$size == 0){
      file_path <- next_file(output_dir)
      next
    }
    encoding <- guess_encoding(file_path)
    if (length(encoding[[1]]) == 0){
      write_lines(file_path, paste0(output_dir, "/cant_read.txt"), append = TRUE)
      file_path <- next_file(output_dir)
      next
    }

    # Load the data contained in the next file from files.txt
    dataframe <- load_file(file_path)

    # Check if the file contains a column consistent with an identifier column
    # with the correct identifier. If not, skip to next file. is_identifier is a
    # logical vector with a True/False value for every column in the dataframe
    is_identefier <- identifier_without_target(dataframe, output_dir, file_path)

    identifiers <- determine_identifiers(dataframe, is_identefier)

    if (!is.null(identifiers)){
      for (identifier in identifiers){
        extract_rest_of_columns(identifier, dataframe, is_identefier, output_dir, file_path)
      }

    } else {
      extract_rest_of_columns(NULL, dataframe, is_identefier, output_dir, file_path)
    }

    # Remove the top file from files.txt and set file_path to the next file to reset the loop.
    file_path <- next_file(output_dir)

  }
  file.remove(paste0(output_dir, "files.txt"))
}

determine_identifiers <- function(dataframe, is_identifier){
  if (sum(is_identifier) > 0){
    identifiers <- unique(sort(unlist(dataframe[is_identifier], use.names = FALSE)))
  } else {
    identifiers <- NULL
  }
  identifiers
}

extract_rest_of_columns <- function(identifier, dataframe, is_identefier, output_dir, file_path){
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


  # Year identifier returns a logical vector with a value for every column in dataframe
  # The value will be TRUE if that column's data is consistent with years (1980-2030).
  is_year <- year_identifier(dataframe)
  if (length(is_year) == 0 | sum(is_year) == 0){
    write_lines(paste0("Identifier: ", identifier, "path: ", file_path),
                paste0(output_dir, "missing_year_needs_investigating.txt"),
                sep = "\n", append = TRUE)
    return()
  }

  is_hour <- hour_identifier(dataframe, is_day, is_year)
  if (length(is_hour[[1]]) > 0 & sum(is_hour[[1]]) > 0){
    return()
  }

  dataframe <- add_date(dataframe, is_day, is_year)

  is_solar <- solar_identifier(dataframe, file_path, output_dir)
  if (length(is_solar) == 0 || sum(is_solar) == 0){
    return()
  }


  # Construct a new dataframe containing only the data identified by the identifier functions.
  solar_dataframe <- construct_solar_data(dataframe,
                                          is_day,
                                          is_year,
                                          is_identefier,
                                          is_solar,
                                          file_path)
  # Create a new file in the output directory containing the snow_depth_dataframe
  save_solar(solar_dataframe, output_dir, is_year)
}



# Find file paths for all files recursively within the file directory and
# write said file paths to files.txt in the output directory. A regex expression can
# optionally be given to narrow down the list, but list.files only applies the regex
# expression to the file name, as opposed to the whole file path, so applying the filter at a
# later stage on the whole file path is likely preferable.
find_files_no_identifier <- function(file_dir, output_dir, pattern = NULL){
  files <- list.files(path = file_dir, full.names = TRUE, recursive = TRUE,
             pattern = pattern, ignore.case = TRUE, include.dirs = FALSE, no.. = FALSE)
  write_lines(files, paste0(output_dir, "/files.txt"), sep = "\n")
}

# Find the position of the column headers, if any, and extract them. Ignore meaningless
# header names ...(number) and X(number). A row is assumed to be a header line if
# columns 2,3, and 4 do not contain numerical data. up to the first 10 rows are checked.
find_header <- function(file_path){
  # number of rows to skip when reading file so that the first row read is the header
  skip <- vector("integer")
  row_number <- 1  # The current row being read
  # While no header has been found and 10 rows have not yet been checked, read the next
  # row and check if it has non-numerical data in columns 2, 3, and 4.
  while (length(skip) == 0 && row_number < 10){
    # read next row of file, all data as characters
    row <- read_csv(file = file_path,
                    skip = row_number - 1,
                    trim_ws = TRUE,
                    n_max = 1,
                    col_names = FALSE,
                    col_types = cols(.default = "c"))
    # Only continue if there are at least 4 columns
    if (length(row) > 3){
      # If columns 2,3 and 4 contain data and not common meaningless header names
      if (!is.na(row[2]) && !is.na(row[3]) && !is.na(row[4]) &&
          !grepl("\\.\\.\\.[0-9]", row[2]) && !grepl("(^|,) *X[0-9] *(,|$)", row[2])){
        # And if all that data is not numeric then set skip to one less row number
        row <- as.numeric(row)
        if (is.na(row[2]) && is.na(row[3]) && is.na(row[4])){skip <- row_number - 1}
      }
    }
    row_number <- row_number + 1
  }
  # If no header line found set skip to "no header"
  if (length(skip) == 0){skip = "no header"}
  skip # Return number of rows to skip when loading file to make the first row the headers
}

# A function to load the data from the next file in files.txt
load_file <- function(file_path){
  # Determine the number of rows to skip such that the first row is the header line
  skip <- find_header(file_path)
  # If header found, read in the marked data into dataframe, else read all data into dataframe. All
  # data read as character class to avoid misleading factors or missing data.
  if (skip != "no header"){
    # If the header does not have a column name for every column with data then read.table will
    # return an error. This will result in dataframe having a length of 0.
    dataframe <- data.frame()
    try(dataframe <- as_tibble(read.table(file = file_path,
                          skip = skip,
                          header = TRUE,
                          colClasses = "character",
                          sep = ",",
                          fill = TRUE,
                          strip.white = TRUE,
                          check.names = TRUE,
                          row.names = NULL,
                          )))

    # If the found header does not cover all data read the csv file as if no header was found
    if (length(dataframe) == 0){
      number_of_cols <- max(
        count.fields(file_path, sep = ",", quote = '""')[!is.na(count.fields(
          file_path, sep = ",", quote = '""'))])
      dataframe <- as_tibble(read.table(file = file_path,
                                        header = FALSE,
                                        col.names = paste0("V", seq_len(number_of_cols)),
                                        colClasses = "character",
                                        sep = ",",
                                        fill = TRUE,
                                        strip.white = TRUE,
                                        check.names = TRUE,
                                        row.names = NULL
      ))
    }
  } else {
    number_of_cols <- max(
      count.fields(file_path, sep = ",", quote = '""')[!is.na(count.fields(
        file_path, sep = ",", quote = '""'))])
    dataframe <- as_tibble(read.table(file = file_path,
                          header = FALSE,
                          col.names = paste0("V", seq_len(number_of_cols)),
                          colClasses = "character",
                          sep = ",",
                          fill = TRUE,
                          strip.white = TRUE,
                          check.names = TRUE,
                          row.names = NULL
                          ))
  }

  dataframe
}

# A function which analyses every column in dataframe and returns a logical vector where
# the value true indicates the corresponding column contains a list of days (1-366).
# A column is identified as containing day data if, ignoring the 1st and last quartiles to avoid
# being thrown by errors, the data ranges from 30 - 366, as well as containing data that, at least 90%
# of the time increases by exactly 1 between unique values.
day_identifier <- function(dataframe){
  is_day <- vector("logical")
  # For each column in dataframe append TRUE to is_day if the column is a list of days
  # and FALSE if it isn't
  for (i in 1:length(dataframe)){
    col <- dataframe[[i]] # set col to next column in dataframe
    col <- as.numeric(col) # convert to class numeric to remove all non-numeric data
    col <- col[!is.na(col)] # remove all NA values
    # if col contained numeric data, examine it to deterine if it is a list of days
    if (length(col) > 0){
      # Squeezed range is the range minus the 1st and last quartiles. This is a conservative
      # method of checking the range that will not permit some rouge data preventing a hit.
      squeezed_range <- unname(quantile(col, na.rm = TRUE))[c(2,4)]
      # Remove repeated data
      col <- unique(col)
      # The calculation for steps will fail if there are not at least two unique values in col
      # Also, there is at least one example of 2 identifier numbers occurring sequentially and thus
      # looking like a day column. It is highly unlikely this will happen 5 times sequentially.
      if (length(col) < 5){
        is_day <- append(is_day, FALSE)
        next
        }
      # Subtract the values of col from itself, offset by one, to determine the step change
      # going from one value to the next
      steps <- col[2:length(col)] - col[1:(length(col) - 1)]
      # Calculate the proportion of step changes in col that are exactly 1.
      one_steps_ratio <- sum(steps == 1)/length(steps)
      # append TRUE to is_day if the 2nd and 3rd quartiles range from 30-366, and the one step
      # ratio is a numeric greater than 0.9, else append FALSE
      if (squeezed_range[1] > 30 &&
          squeezed_range[2] < 366 &&
          !is.na(one_steps_ratio) &&
          one_steps_ratio > 0.9){
        is_day <- append(is_day, TRUE)
      } else {is_day <- append(is_day, FALSE)}
    } else {is_day <- append(is_day, FALSE)}
  }
  # There should only be one day column. If more than one is found then data has likely been
  # miss-attributed. A likely cause is a column which merely counts the row number (present in
  # many of the data files), which always increases by 1 and is generally the first column of a sheet.
  # Given that the data rarely starts at the beginning of a calender year, if more than one day column
  # is detected, discard column one, then a column starting at 1, if this is only one of multiple columns.
  if (sum(is_day) > 1){is_day[1] = FALSE} # If more than one column discard column one
  if (sum(is_day) > 1){ # If still more than one column find how many begin at 1
    starts_at_one <- vector("integer")
    # For every column in dataframe identified as a Day column, check that the first value in column
    # is not NA and is == 1. If so, append i (the count of columns identified as Day, not the index
    # is_day) to starts_at_one
    for (i in 1:sum(is_day)){
      if (!is.na(dataframe[is_day][[i]][1]) &&
          dataframe[is_day][[i]][1] == 1){starts_at_one <- append(starts_at_one, i)}
    }
    # If exactly one of the multiple columns identified as day begins with one, discard it
    # If there are more than one then return them all, uncertainty too great to choose
    if (length(starts_at_one) == 1){
      # For every value in is_day, sequentially sum all values to that index, until the
      # nth value corresponding to starts_at_one is reached. Set that value to FALSE
      for (i in 1:length(is_day)){
        if (sum(is_day[1:i]) == starts_at_one){
          is_day[i] = FALSE
          break
        }
      }
    }
  }
  is_day
}

# A function which analyses every column in dataframe and returns a logical vector where
# the value true indicates the corresponding column contains a list of years.
# A column is identified as containing year data if, ignoring all 6999 values, NAs,
# non-numeric data, and the 1st and last quartiles, the data ranges from 1980-2030
# and there are at least 10 data points for every unique value identified.
year_identifier <- function(dataframe){
  is_year <- vector("logical")
  for (i in 1:length(dataframe)){
    col <- dataframe[[i]]
    col <- col[!grepl("6999", col)]
    col <- as.numeric(col)
    col <- col[!is.na(col)]
    if (length(col) > 0){
      # squeezed range is the range missing the first and last quartiles
      squeezed_range <- unname(quantile(col))[c(2,4)]
      # unique ratio is a measure of how little the variable changes. There should be
      # many results per year, so a low unique ratio is expected.
      unique_ratio <- length(unique(col))/length(col)
      if (squeezed_range[1] > 1980 &&
          squeezed_range[2] < 2030 && # 2030 was chosen to future proof the script
          unique_ratio < 0.1 ||
          squeezed_range[1] > 1980 &&
          squeezed_range[2] < 2030 &&
          squeezed_range[1] == squeezed_range[2]){is_year <- append(is_year, TRUE)}
      else {is_year <- append(is_year, FALSE)}
    } else {is_year <- append(is_year, FALSE)}
  }
  is_year
}

add_date <- function(dataframe, is_day, is_year){
  date <- ymd(paste0(dataframe[is_year][[1]], "0101"))
  date <- `yday<-`(date, as.numeric(dataframe[is_day][[1]]))
  dataframe <- dataframe %>% mutate(date = date)
}

# A function to identify the identifier column without looking for a specific identifier number.
# The identifier column contains a numerical value that describes the type of data to expect
# on that row. A column is determined to be an identifier column if at least 80% of its values
# consist of only either 2 or 3 digits, and it is mostly a repetition of the same few unique
# values. The function returns a logical vector with TRUE/FALSE values for every column in dataframe.
identifier_without_target <- function(dataframe, output_dir, file_path){
  is_identifier <- vector("logical")
  # The identifier column always occurs near the start of the file. Check only the first
  # five columns, or length of the dataframe, whichever is smaller
  if (length(dataframe) < 5){
    search_range <- length(dataframe)
  } else {
    search_range <- 5
  }
  # For each column to be checked for identifier data, check if the ratio of values in that column
  # which consist of either 2 or 3 digits only is at least 80%. Also check that the data contains only
  # a few unique values. If so, append TRUE to is_identifier, else FALSE.
  for (i in 1:search_range){
    if (sum(grepl('(^|,)"?[0-9][0-9][0-9]?"?(,|$)', dataframe[[i]]))/length(dataframe[[i]]) < 0.8) {
      is_identifier <- append(is_identifier, FALSE)
      next}
    unique_ratio <- length(unique(sort(dataframe[[i]])))/length(dataframe[[i]])
    if (unique_ratio > 0.05){
      is_identifier <- append(is_identifier, FALSE)
    } else {
        is_identifier <- append(is_identifier, TRUE)}
  }

  # If multiple columns are identified as identifier columns, take only the one with the
  # lowest index, but write the path to file for manual validation
  identifier_indexes <- which(is_identifier == TRUE, arr.ind = TRUE)
  if (length(identifier_indexes) > 1){
    write_lines(file_path,
                paste0(output_dir, "/multiple_potential_identifier_columns.txt"),
                append = TRUE,
                sep = "\n")
  }
  # ensure that is_identifier has a logical value for every column in dataframe, and there
  # is at most one TRUE value, indicating the first column meeting the is_identifier criteria.
  is_identifier <- rep(FALSE, length(dataframe))
  if (length(identifier_indexes) > 0){
    is_identifier[identifier_indexes[1]] <- TRUE
  }

  is_identifier
}

hour_identifier <- function(dataframe, is_day, is_year){
  is_hour <- vector("logical")
  if (length(dataframe) < 8){
    search_range <- length(dataframe)
  } else {
    search_range <- 8
  }
  i <- 1
  while((length(is_hour) == 0 | sum(is_hour) == 0) &  i < search_range){
    col <- dataframe[[i]]
    correct_format_ratio <- sum(grepl("^[0-2]?[0-9][0-5]0$", col))/length(col)
    if(correct_format_ratio > 0.8){
      col <- ifelse(grepl("^.[0-5][0-9]$", col), paste0("0", col), col)
      datetime <- ymd_hm(paste0(dataframe[is_year][[1]], "0101", col))
      datetime <- `yday<-`(datetime, as.numeric(dataframe[is_day][[1]]))
      hour <- hour(datetime)
      hour <- unique(sort(hour))

      if (length(hour) > 23){
        is_hour <- append(is_hour, TRUE)
        dataframe <- mutate(dataframe, fractional_day = yday(datetime) +
                              (hour(datetime) + minute(datetime)/60)/24)
        is_day <- append(is_day, FALSE)
        is_year <- append(is_year, FALSE)

      } else {
        is_hour <- append(is_hour, FALSE)
      }

    } else {
      is_hour <- append(is_hour, FALSE)
    }
    i <- i + 1
  }
  is_hour <- append(is_hour, rep(FALSE, length(dataframe) - length(is_hour)))
  is_hour <- list(is_hour, dataframe)
  is_hour
}

solar_identifier <- function(dataframe, file_path, output_dir){
  is_solar <- vector("logical")
  dataframe <- dataframe %>% convert(num(names(dataframe[1:(length(dataframe) - 1)])))
  for (i in 1:(length(dataframe) - 1)){
    dataframe_i <- subset(dataframe, !grepl("6999", dataframe[[i]]))
    dataframe_i <- subset(dataframe_i, !is.na(dataframe_i[[i]]))

    if (length(dataframe_i[[i]]) == 0){
      is_solar <- append(is_solar, FALSE)
      next}
    squeezed_range <- unname(quantile(dataframe_i[[i]], na.rm = TRUE))
    if (is.na(squeezed_range[2]) |
        is.na(squeezed_range[4]) |
        squeezed_range[2] < 0 |
        squeezed_range[4] > 1000 |
        # Some files contain columns containing only 0's, which can appear like solar data
        (squeezed_range[1] == 0 & squeezed_range[5] == 0)) {
      is_solar <- append(is_solar, FALSE)
      next}
    winter_months <- dataframe_i %>% filter(month(date) %in% c(11, 12, 1))
    if(length(winter_months[[i]]) > 0){
      is_zero_ratio <- length(winter_months[[i]][winter_months[[i]] < 0.01])/length(winter_months[[i]])
      if(is_zero_ratio < 0.8){
        is_solar <- append(is_solar, FALSE)
        next}
    }
    summer_months <- dataframe_i %>% filter(!(month(date) %in% c(10, 11, 12, 1, 2, 3)))
    if(length(summer_months[[i]]) > 30){
      if (median(summer_months[[i]]) < 100){
        is_solar <- append(is_solar, FALSE)
        next
      }
      model <- lm(summer_months[[i]] ~
                    sin(2*pi*(365*year(summer_months$date) + yday(summer_months$date))/365) +
                    cos(2*pi*(365*year(summer_months$date) + yday(summer_months$date))/365))
      r2 <- summary(model)$r.squared
      if(!is.na(r2) & r2 < 0.5){
        is_solar <- append(is_solar, FALSE)
        next
      }
    }
    difficult_months <- dataframe_i %>% filter(month(date) %in% c(10,2,3))
    difficult_percent <- 100*length(difficult_months[[i]])/length(dataframe_i[[i]])
    if (difficult_percent > 80){
      write_lines(paste0(file_path, ": ", difficult_percent, "% difficult months"),
                  file = paste0(output_dir, "cant_determine.txt"),
                  sep = "\n",
                  append = TRUE)
      next
    }
      is_solar <- append(is_solar, TRUE)


  }
  is_solar
}

# A function which accepts a dataframe, and the logical vectors is_day, is_year,
# is_identifier, is_snow, and is_site, and uses these logical vectors to produce
# a new dataframe containing only Identifier, Year, Day, Snow_depth, and site data.
# Finally, it creates a new column, file, detailing the file path to the original data.
construct_solar_data <- function(dataframe, is_day, is_year,  is_identifier,
                                is_solar, file_path){

  solar_dataframe <- dataframe %>% select(colnames(dataframe)[is_identifier],
                                          date,
                                          colnames(dataframe)[is_year],
                                          colnames(dataframe)[is_day],
                                          colnames(dataframe)[is_solar]) %>%
    # Create new column, file, detailing where the original data was found
    mutate(file = file_path)

  solar_dataframe
}

# A function which takes the snow depth dataframe produced by construct_snow_data
# and writes it to file. It names the file as claude_snow_depth_YEAR-YEAR.csv, if
# the file spans multiple years, or claude_snow_depth_YEAR_DAY-DAY.csv if it doesn't
# If a file with the same name already exists, an iterator is added to the end. Files
# are written to the output directory
save_solar <- function (solar_dataframe, output_dir, is_year){
  if (length(is_year) > 0 && sum(is_year) > 0){ #If a year column has been identified
    years <- solar_dataframe[[3]] # Year will be the second column
    years <- as.numeric(years)
    years <- years[!is.na(years)]
    first_year <- years[1]
    last_year <- years[length(years)]
  } else {
    first_year <- "unknown_year"
    last_year <- "unknown_year"
  }
  # If the file spans more than one year name file: claude_snow_depth_YEAR1-YEAR2.csv
  if (first_year != last_year){
    file_name <- paste0(output_dir, "/claude_solar_",as.character(first_year),
                        "-", as.character(last_year), ".csv")
  } else {
    # If the data is all from one year, determine the first and last day and name file
    # claude_snow_depth_YEAR_day_DAY1-DAY2.csv
    days <- solar_dataframe[[4]]
    days <- as.numeric(days)
    days <- days[!is.na(days)]
    first_day <- days[1]
    last_day <- days[length(days)]
    file_name <- paste0(output_dir, "/claude_solar_",as.character(first_year),
                        "_day_", as.character(first_day), "-", as.character(last_day), ".csv")
  }
  # If file name already exists in directory, iterate with count such that file is
  # now named file_name_count.csv
  count <- 1
  original_file_name <- file_name
  while (file.exists(file_name)){
    file_name <- sub("\\.csv", paste0("_", count, ".csv"), original_file_name)
    count <- count + 1
  }
  write_csv(solar_dataframe, file_name)
}
# A function to read the next line from the text document files.txt (which contains
# all the file paths for files that were found to contain the target searched for).
# The function also removes the previous file path from files.txt, which has now been
# processed. It returns the file path to process next
next_file <- function(output_dir){
  # Read all of files.txt minus the first line into variable files
  files <- read_lines(paste0(output_dir, "/files.txt"), skip = 1)
  # Overwrite files.txt with this updated file list
  write_lines(files, paste0(output_dir, "files.txt"), sep = "\n")
  # Set file path to the top file of the new list
  file_path <- files[1]
  file_path
}
create_file_reader_function <- function(file_path){
  file_list <- read_lines(file_path, skip_empty_rows = TRUE)
  file_list <- sub(".*: *", "", file_list)
  number_of_files <- length(file_list)
  file_number <- 1
  next_file <- function(){
    print(paste0("file ", file_number, "of ", number_of_files))
    print(file_list[file_number])
    dataframe <- load_file(file_list[file_number])
    if (file_number < number_of_files){
      file_number <<- file_number + 1
    } else {
      file_number <<- 1
    }
    dataframe
  }
  return(next_file)
}


#Colate_claude_solar()

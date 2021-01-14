#!/bin/env Rscript

library(tidyverse)

#A function designed to extract snow depth data from an entire nested directory 
#of CSV files. It searches for a target identifier (239 in the case of Cassiope),
#returning a list of all files containing the number in at least one cell anywhere
#in the document. It writes this list of files to the file files.txt. It then takes
#each file in turn and checks if they truly do contain snow depth data. First it
# finds all the columns containing the identifier 239, and checks if the rows containing
# 239 also contain plausible snow depth data. If there are multiple columns containing
#the number 239 that line up with plausible snow depth data, the column is chosen which
#has the greatest number of 239s.
#Having identified the identifier column, the data is now simplified to only contain
#those rows with a 239 identifier. A day column is then identified as one containing 
#only numbers between 1 and 366, and who's numbers, when repetition is eliminated,
#almost exclusively increase in increments of 1.
#The snow depth data is identified as a column which which has both 25% and 75% quartile
#measures between 800 and 1800. If any of the columns: Identifier, Day, or Snowdepth,
#can not be identified then this file is disregarded and the next file in files.txt is
#analysed.
# If Identifier, Day, and Snowdepth are identified then, after attempting to find the
# year column, a new dataframe is created containing only the data corresponding to an
# identifier of 239, and only columns containing Identifier, Year, Day, and snow depth,
# as well as the path to the original file from which the data was collected. This new
# dataframe is written to file in the chosen output directory

Colate_snow_depth <- function(){
  # The directory that shall be recursively searched for snowdepth data
  file_dir = "./duplicateCSV/"
  # The directory in which all results shall be stored
  output_dir = "./snowdepth/"
  if (!dir.exists(output_dir)){dir.create(output_dir)}
  # A REGEX expression describing the desired identifier as the sole data in a 
  #spreadsheet cell, with any amount of white space permitted, as well as optional
  #quotes around the number
  target = "'(^|,) *\"?239\"? *(,|$)'"
  #call the function to create a file containing a list of all CSV files that have
  #at least one cell matching the target
  if (!file.exists(paste0(output_dir, "/files.txt"))){
  find_files(file_dir, output_dir, target)
  }
  #read the first file path contained in the file created by find_files
  file_path <- read_lines(paste0(output_dir, "/files.txt"), n_max = 1)
  # While there is still files left to check in files.txt, attempt to identify columns
  # containing Identifiers, Years, Days, and Snow Depth Data. If appropriate data is
  # found, save it to a new file in the output directory before moving on to the next
  # file in files.txt
  while (!is.na(file_path)){
    print(file_path) #Allows the monitoring of the scripts progress while running
    # Some files contain characters which R can not read. If this file contains
    # such characters, make a note of it and skip to next file
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
    is_identefier <- identifier_identifier(dataframe)
    if (length(is_identefier) == 0 || sum(is_identefier) == 0){
      file_path <- next_file(output_dir)
      next
    }
    # Remove all data not pertaining to Identifier 239
    dataframe <- filter(dataframe, grepl('(^|,) *"?239"? *(,|$)', dataframe[is_identefier][[1]]))
    
    # Day identifier returns a logical vector with a value for every column in dataframe
    # The value will be TRUE if that column's data is consistent with Days (1-366).
    is_day <- day_identifier(dataframe)
    if (length(is_day) == 0 || sum(is_day) == 0){
      file_path <- next_file(output_dir)
      next
    }
    
    # Snow identifier returns a logical vector with a value for every column in dataframe
    # The value will be TRUE if that column's data is consistent with snow depth.
    is_snow <- snow_depth_identifier(dataframe, is_identefier)
    if (length(is_snow) == 0 || sum(is_snow) == 0){
      no_snow_list(file_path, output_dir)
      file_path <- next_file(output_dir)
      next
    }
    # Year identifier returns a logical vector with a value for every column in dataframe
    # The value will be TRUE if that column's data is consistent with years (1980-2030).
    is_year <- year_identifier(dataframe)
    # Site identifier returns a logical vector with a value for every column in dataframe
    # The value will be TRUE if that column's data is consistent with site names.
    # No such data seems to be available.
    is_site <- site_identifier(dataframe)
    
    # Construct a new dataframe containing only the data identified by the identifier functions.
    snow_depth_dataframe <- construct_snow_data(dataframe,
                                                is_day,
                                                is_year,
                                                is_identefier,
                                                is_snow,
                                                is_site,
                                                file_path)
    # Create a new file in the output directory containing the snow_depth_dataframe
    save_snow_depth(snow_depth_dataframe, output_dir, is_year, is_identefier)
    
    # Remove the top file from files.txt and set file_path to the next file to reset the loop.
    file_path <- next_file(output_dir)
    
  }
}


# Find file paths for all files within the file directory that contain the target and
# write said file paths to files.txt in the output directory. The code relies on the 
# underlying unix OS. The sed statement removes all extraneous data returned by grep.
find_files <- function(file_dir, output_dir, target){
  system(paste0("grep -roE ", target, " ", file_dir, "|sed s/:.*// > ",
                output_dir,"/files.txt"))
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
    # Only continue if there are atleast 4 columns
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
    dataframe <- read_csv(file = file_path,
                          skip = skip, 
                          trim_ws = TRUE,
                          col_names = TRUE,
                          col_types = cols(.default = "c"))
  } else {
  dataframe <- read_csv(file = file_path,
                        trim_ws = TRUE,
                        col_names = FALSE,
                        col_types = cols(.default = "c"))
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

# A function to identify the identifier column. The identifier column contains a numerical
# value that describes the type of data to expect on that row. For Cassiope, 239 
# indicates snow depth data. A column is determined to be an identifier column if
# it contains at least one instance of 239, and those rows result in snow
# depth data detected in some other columns. The function returns a logical vector with
# TRUE/FALSE values for every column in dataframe.
identifier_identifier <- function(dataframe){
  is_identifier <- vector("logical")
  # For every column in dataframe append TRUE to is_identifier if the column contains
  # at least one instance of 239, else append FALSE
  for (i in 1:length(dataframe)){
    if (sum(grepl("(^|,) *239 *(,|$)", dataframe[[i]])) > 0){
      is_identifier <- append(is_identifier, TRUE)} else {
        is_identifier <- append(is_identifier, FALSE)}
  }
  # For every identified potential identifier column, check if associated snow depth
  # data can be found for it. Only return TRUE values with associated snow depth data
  is_identifier <- identifier_confirmer(dataframe, is_identifier)
  is_identifier
  }

# A function that checks every potential identifier column found by the identifier 
# identifier function. It checks if corresponding snow depth data can be found for
# each instance of the target being hit. If multiple columns return possible snow depth
# data, the column with the most 239 hits is returned as the identifier column. A likely
# example of a column that would return snow depth data, contain 239, but not be a true
# identifier column is Day. The function returns a logical vector with a value for every
# column in dataframe, with at most one TRUE value corresponding to the determined 
# Identifier column.
identifier_confirmer <- function(dataframe, is_identifier){
  # indicies of dataframe for potential Identifier columns for which snow data is found
  id_indicies <- vector("integer") 
  sum_identifiers <- sum(is_identifier) # number of potential identifier columns
  # For every potential identifier column, check for corresponding snow depth data
  # If found, append dataframe index to id_indicies. Whether, snow depth data is found
  # or not, change is_identifier value to FALSE to allow the loop to progress to the 
  # next potential Identifier column
  for (i in 1:sum_identifiers){
    is_snow <- snow_depth_identifier(dataframe, is_identifier)
    if (sum(is_snow) > 0){ # If any corresponding snow depth data is found
      # Set column number to the index of the first value within is_identifier with the
      # value TRUE. This corresponds to the index for the dataframe column analysed for
      # snow depth data.
      column_number <- first_true_index(is_identifier)
      id_indicies <- append(id_indicies, column_number)
      is_identifier <- id_to_false(is_identifier) # Set the current is_identifier value to FALSE
    } else {
      is_identifier <- id_to_false(is_identifier) # Set the current is_identifier value to FALSE
    }
  }
  # If no Identifier column detected, return is_identifier with all FALSE values
  if (length(id_indicies) == 0){
    is_identifier <- rep(FALSE, length(dataframe))
    return(is_identifier)
  }
  # highest number of target hits out of all potential Identifier columns
  max_id_hits <- vector("integer") 
  # The dataframe column index that gave max_id_hits
  max_hits_index <- vector("integer")
  # count the number of target hits for each of the potential identifier columns. update
  # max_id_hits and max_hits_index if more hits are found than any column checked so far
  for (i in 1:length(id_indicies)){
    hits <- length(dataframe[[id_indicies[i]]][grepl('(^|,) *"?239"? *(,|$)',
                                                     dataframe[[id_indicies[i]]])])
    if (length(max_id_hits) > 0 && max_id_hits > hits){next} else {
      max_id_hits <- hits
      max_hits_index <- i
    }
  }
  # Create a logical vector with a FALSE value for every column in dataframe
  is_identifier <- rep(FALSE, length(dataframe))
  # Change the value corresponding to the column identified as identifier to TRUE
  is_identifier[max_hits_index] <- TRUE
  is_identifier
}

# A function which accepts a logical vector, and returns the index of the first 
# value of that vector which equals TRUE
first_true_index <- function(is_x){
  running_total <- 1
  for (i in 1:length(is_x)){
    running_total <- running_total + is_x[i]
    if (running_total == 2){
      index <- i
      break
    }
  }
  index
}

# A function which accepts a logical vector, and changes the first TRUE value to FALSE
id_to_false <- function(is_identifier){
  identifier_index <- first_true_index(is_identifier)
  is_identifier[identifier_index] <- FALSE
  is_identifier
}

# This function takes a dataframe and a logical vector that identifies which column within
# the dataframe contains identifier information signaling which rows have snow depth data.
# It isolates all the rows that are marked by the identifier target (239 for Cassiope), then
# loops through each of the columns in dataframe, discarding all values of 6999 (this is an 
# error code) and all NA or non-numeric data. Of the remaining data, if the 25%, 50%, and 75%
# all lie within the range 700-1800, then the column is considered snow depth data and TRUE is
# appended to is_snow, else FALSE is appended. This logical vector is returned.

snow_depth_identifier <- function(dataframe, is_identifier){
  # Subset the dataframe to only the rows where the identifier column matches the target
  # If there are multiple possible identifier columns the first one is used.
  possible_snow <- grepl('(^|,) *"?239"? *(,|$)', dataframe[is_identifier][[1]])
  is_snow <- vector("logical")
  # For every column in dataframe, remove all rows that do not contain the target within
  # the identifier column, contain the value 6999, NA, or aren't numeric. Examine the remaining
  # data, and if the 25%, 50%, and 75% levels are within the range 700-1800, append TRUE to
  # is_snow, else append FALSE
  for (i in 1:length(dataframe)){
    col <- dataframe[[i]][possible_snow]
    col <- col[!grepl("6999", col)]
    col <- as.numeric(col)
    col <- col[!is.na(col)]
    if (length(col) > 0){
    quantiles <- unname(quantile(col))[2:4]
    if (quantiles[1] > 700 && quantiles[1] < 1800 &&
        quantiles[2] > 700 && quantiles[2] < 1800 &&
        quantiles[3] > 700 && quantiles[3] < 1800){
      is_snow <- append(is_snow, TRUE)
    } else {is_snow <- append(is_snow, FALSE)}
    } else {is_snow <- append(is_snow, FALSE)}
  }
  is_snow
}

year_identifier <- function(dataframe){
  is_year <- vector("logical")
  for (i in 1:length(dataframe)){
    col <- dataframe[[i]]
    col <- col[!grepl("6999", col)]
    col <- as.numeric(col)
    col <- col[!is.na(col)]
    if (length(col) > 0){
      squeezed_range <- unname(quantile(col))[c(2,4)]
      unique_ratio <- length(unique(col))/length(col)
      if (squeezed_range[1] > 1980 &&
          squeezed_range[2] < 2030 &&
          unique_ratio < 0.1 ||
          squeezed_range[1] > 1980 &&
          squeezed_range[2] < 2030 &&
          squeezed_range[1] == squeezed_range[2]){is_year <- append(is_year, TRUE)}
      else {is_year <- append(is_year, FALSE)}
    } else {is_year <- append(is_year, FALSE)}
  }
  is_year
}

construct_snow_data <- function(dataframe, is_day, is_year, is_identifier,
                                is_snow, is_site, file_path){
  
  snow_depth_dataframe <- dataframe %>% select(colnames(dataframe)[is_identifier],
                                               colnames(dataframe)[is_year],
                                               colnames(dataframe)[is_day],
                                               colnames(dataframe)[is_snow],
                                               colnames(dataframe)[is_site]) %>%
    filter(grepl('(^|,) *"?239"? *(,|$)', dataframe[is_identifier][[1]])) %>%
    mutate(file = file_path)
  
  snow_depth_dataframe
}

save_snow_depth <- function (snow_depth_dataframe, output_dir, is_year, is_identifier){
  if (length(is_year) > 0 && sum(is_year) > 0){
  years <- snow_depth_dataframe[[2]]
  years <- as.numeric(years)
  years <- years[!is.na(years)]
  first_year <- years[1]
  last_year <- years[length(years)]
  } else {
    first_year <- "unknown_year"
    last_year <- "unknown_year"
  }
  if (first_year != last_year){
  file_name <- paste0(output_dir, "/cassiope_snow_depth_",as.character(first_year),
                      "-", as.character(last_year), ".csv")
  } else {
    days <- snow_depth_dataframe[[2 + sum(is_year)]]
    days <- as.numeric(days)
    days <- days[!is.na(days)]
    first_day <- days[1]
    last_day <- days[length(days)]
    file_name <- paste0(output_dir, "/cassiope_snow_depth_",as.character(first_year),
                        "_day_", as.character(first_day), "-", as.character(last_day), ".csv")
  }
  count <- 1
  original_file_name <- file_name
  while (file.exists(file_name)){
    file_name <- sub("\\.csv", paste0("_", count, ".csv"), original_file_name)
    count <- count + 1
  }
  write_csv(snow_depth_dataframe, file_name)
}

site_identifier <- function(dataframe){
  is_site <- vector("logical")
  for (i in 1:length(dataframe)){
    col <- dataframe[[i]]
    site_hits <- grepl("[Cc].?[Aa].?[Ss]", col)
    if (sum(site_hits) > 10){
      is_site <- append(is_site, TRUE)
    } else {is_site <- append(is_site, FALSE)}
  }
  is_site
}
no_snow_list <- function(file_path, output_dir){
  write_lines(file_path, paste0(output_dir, "/no_snow_columns.txt"), append = TRUE)
}

next_file <- function(output_dir){
  files <- read_lines(paste0(output_dir, "/files.txt"), skip = 1)
  write_lines(files, paste0(output_dir, "files.txt"), sep = "\n")
  file_path <- files[1]
  file_path
}

#Colate_snow_depth()

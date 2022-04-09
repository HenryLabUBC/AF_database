library(tidyverse)
library(tundra)
library(lubridate)


search_dir <- "/home/ross/Desktop/hobo_converted/"
output_dir <- "/home/ross/Desktop/hobo"

convert_tab_delim_files <- function(search_dir, output_dir, file_paths = NA){
  file_status <- list(
    not_processed = vector("character"),
    processed = vector("character"),
    extra_data_cols = vector("character"),
    too_many_cols = vector("character"),
    cant_load = vector("character"),
    no_header = vector("character"),
    no_number_col = vector("character"),
    no_date_col = vector("character")
  )
  # For troubleshooting purposes it is possible to pass a file_path directly
  if (is.na(file_paths)){
    file_paths <- list.files(search_dir,
                             full.names = TRUE,
                             recursive = TRUE
    )
  }
  for (file_path in file_paths){
    print(file_path)
    exclude_list <- c("/media/ross/BackupPlus/Hoba_data/Hobos2/non_csv_files//Dome/2002/dome_temps.xls",
                      "/home/ross/Desktop/csv//dome_temps.xls",
                      "/home/ross/Desktop/csv//dome_temps.xlsx",
                      "/home/ross/Desktop/hobo_converted//Meadow/2010/MEAD_C7.TXT"
    )
    if (file_path %in% exclude_list){next}
    dataframe <- data.frame()
    try(dataframe <- load_file(file_path, sep = "", skip = 0))
    
    if (is.data.frame(dataframe)){
      if (nrow(dataframe) == 0){
        file_status$not_processed <- append(file_status$not_processed, file_path)
        file_status$cant_load <- append(file_status$cant_load, file_path)
        next
      }
      file_status <- process_dataframe(dataframe, file_status, file_path)
    } else {
      for (df in dataframe){
        file_status <- process_dataframe(df, file_status, file_path)
      }
    }
    
    
  }
  file_status
}

process_dataframe <- function(dataframe, file_status, file_path){
  i <- 0
  while(sum(grepl("date", names(dataframe))) == 0){
    if (i == 5){
      file_status$not_processed <- append(file_status$not_processed, file_path)
      file_status$no_header <- append(file_status$no_header, file_path)
      
      return(file_status)
    }
    dataframe <- janitor::row_to_names(dataframe, row_number = 1)
    names(dataframe) <- tolower(names(dataframe))
    dataframe <- janitor::clean_names(dataframe)
    i <- i + 1
  }
  # Process all files in standard date, time, temp format
  if ("date" %in% names(dataframe) & "time" %in% names(dataframe) & 
      ("temperature" %in% names(dataframe) | "temp" %in% names(dataframe))){
    non_temp_cols <- names(dataframe)[!names(dataframe) %in% c("date", "time", "temperature", "temp", "f")]
    extra_data_col_count <- 0
    
    for (col in non_temp_cols){
      if (sum(!is.empty(dataframe[[col]])) > 2){
        file_status$extra_data_cols <- append(file_status$extra_data_cols, col)
        extra_data_col_count <- extra_data_col_count + 1
        
      }
    }
    if (extra_data_col_count > 0){
      file_status$not_processed <- append(file_status$not_processed, file_path)
      file_status$too_many_cols <- append(file_status$too_many_cols, file_path)
      return(file_status)
    }
    number = "#"
    dataframe <- mutate(dataframe, date = paste(date, time), 
                        "{number}" := 1:nrow(dataframe))
    dataframe <- select(dataframe, all_of(matches("#")), date, one_of("temperature", "temp"))
    file_name <- sub(".*/", "", file_path)
    write_with_structure(search_dir, output_dir, file_path, dataframe, file_name)
    
    file_status$processed <- append(file_status$processed, file_path)
  } else {
    # confirm first column is a counting number and rename "#", else update file status$no_number_col
    if (dataframe[[1]][1] == "1"){
      # Check at least 90% of the contents of the column contain numbers incrementing by one
      step_change <- as.numeric(dataframe[[1]][2:nrow(dataframe)]) - as.numeric(dataframe[[1]][1:(nrow(dataframe) - 1)])
      if (sum(step_change == 1)/length(step_change) >= 0.9){
        names(dataframe)[1] <- "#"
      } else {
        file_status$not_processed <- append(file_status$not_processed, file_path)
        file_status$no_number_col <- append(file_status$no_number_col, file_path)
        return(file_status)
      }
    } else {
      file_status$not_processed <- append(file_status$not_processed, file_path)
      file_status$no_number_col <- append(file_status$no_number_col, file_path)
      return(file_status)
    }
    
    # Confirm column 2 contains date_time data else update status$no_date
    date_times <- lubridate::mdy_hms(dataframe[[2]])
    if (sum(is.na(date_times))/length(date_times) >= 0.9){
      # Some of the date-times have had ".0" appended to the ends of them. If the dates
      # fail to parse then try removing ".0" from the end and try again
      dataframe[,2] <- sub("\\.0$", "", dataframe[,2])
      date_times <- lubridate::mdy_hms(dataframe[[2]])
      # If date still doesn't parse then update status and move on
      if (sum(is.na(date_times))/length(date_times) >= 0.9){
        file_status$not_processed <- append(file_status$not_processed, file_path)
        file_status$no_date_col <- append(file_status$no_date_col, file_path)
        return(file_status)
      }
    }
    # rename the date column
    names(dataframe[2]) <- "date"
    # create file for each data column
    file_name <- sub(".*/", "", file_path)
    file_name <- sub("\\.csv", "", file_name)
    for (i in 3:length(dataframe)){
      if (sum(is.empty(as.numeric(dataframe[[i]])))/length(dataframe[[i]]) <= 0.5){
        # name for new file
        new_file_name <- paste0(file_name, ".(", names(dataframe[i]), ").csv")
        # write file to disk maintaining file structure
        write_with_structure(search_dir, output_dir, file_path, dataframe[,c(1,2,i)], new_file_name)
        
      }
    }
    file_status$processed <- append(file_status$processed, file_path)
    
  }
  file_status
}

write_with_structure <- function(root_dir, output_dir, old_file_path, dataframe, file_name){
  # determine file structure beyond the root directory
  post_root_structure <- sub(root_dir, "", old_file_path)
  # new_dir
  new_dir <- paste0(output_dir, "/", post_root_structure, "/")
  # create new_dir if it doesn't already exist
  if (!dir.exists(new_dir)){
    dir.create(new_dir, recursive = TRUE)
  }
  # write the data to the output_dir
  write.csv(dataframe, paste0(new_dir, file_name), sep = ",")
}
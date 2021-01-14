# This script can be used to compile data for a given header name from across all
# the files in a given directory that it is found.


library(tidyverse) # tidyverse contains functions that make manipulating tabular data easier

# A function which uses the underlying unix system to search through an entire nested
# directory for files containing a certain word or words. It creates a tibble of all
# the line numbers were hits were found,the file paths that led to a hit, a string
# containing the exact hit, and the number of rows should be skipped to make
# this hit the first line read. It returns this tibble.
get_file_data <- function(target, file_dir, script_output_dir){
  # grep for the appropriate strings and output results to files.txt
  system(paste0("grep -niroE ", target, " ", 
                file_dir, " > ", script_output_dir, "/files.txt"))
  # read the file just created and store as a tibble
  file_data <- as_tibble(read.table(paste0(script_output_dir, "/files.txt"), sep = ":"))
  
  colnames(file_data) <- c("file", "line_num", "text_found") # naming the columns
  
  # convert the file paths from factors to character strings and add a column containing
  # the number of lines to skip to read the csv file with this line as its header
  file_data <- file_data %>% mutate(file = as.character(file), skip = as.integer(line_num - 1))
  file_data
}
# A function which takes the tibble created by get_file_data() and determines which,
# if any hits it can confidently say lie on the data's header. It does this by reading
# the lines in question and checking if they also contain a cell with another expected
# column name. A list of all files where no potential header is detected is written to
# missed_files.txt, while files containing multiple headers is written to multiple_header_hits.txt
# In each case, the files are excluded from further processing. The function returns
# a vector of the indicies of the file_data that result in successful identification
# as the line containing the data header.
determine_correct_hit <- function(file_data, script_output_dir){
  correct_hits <- vector(mode = "integer")
  found_hit <- FALSE
  missed_files <- vector(mode = "character")
  # This loop cycles through each of the hits, reads the individual lines containing
  # the hit, and looks for the presence of another known column header name. The file_data
  #index of each match is stored in the correct_hits vector. If it has reached the last hit
  # of a given file without identifying a match then it appends that file name to 
  # the missed_files vector.
  for (i in seq_along(file_data$file)){
    line <- read_lines(file_data$file[i], skip = file_data$skip[i], n_max = 1) #read line from file
    # check for empty cell containing the name of an expected header name
    header_line <- grepl("(^|,) *thing *(,|$)", line, ignore.case = TRUE) 
    if (header_line == TRUE){ # If line contains an expected column name add its index to correct_hits
      correct_hits <- append(correct_hits, i)
      found_hit <- TRUE
    }
    if (i < length(file_data$file)){ # i.e. if not the last row of table
      if (file_data$file[i] != file_data$file[i+1] & found_hit == FALSE){
        # if next file name differs from the current and a hit still isn't found
        missed_files <- append(missed_files, file_data$file[i]) #add file name to missed_files
      }
      # If the next hit belongs to a different file and a hit was successfully found for
      # the current file, reset the found_hit boolean
      if (file_data$file[i] != file_data$file[i+1] & found_hit == TRUE){
        found_hit <- FALSE
      }
      # if at the last row found_hit == False, add the last file name to missed_files
    } else if (found_hit == FALSE){missed_files <- append(missed_files, file_data$file[i])}
  }
  # If there were missed files, display warning and write missing file names to file
  if (length(missed_files) != 0){
    warning("Failed to find header information in file(s) containing a hit for the
            search criteria. The file(s) were thus excluded. Details can be found in the 
            file no_header_hits.txt")
    writeLines(paste("Failed to find header information in file(s) listed below,
    depite them containing a hit for the search criteria. The file(s) were thus excluded.",
                     paste(missed_files, collapse  = "\n"), sep = "\n"),
               con = paste0(script_output_dir, "/no_header_hits.txt"))
  }
  # If the list of identified headers contains the same file more than once
  if (sum(duplicated(file_data$file[correct_hits])) > 0){
    warning("unable to distinguish between different posible headers in certain #display warning
              files. See multiple_header_hits.txt for details")
    # create a single string containing the names of all duplicated files
    duplicate_file_names <- lapply(file_data$file[correct_hits][duplicated(file_data$file[correct_hits])],
                                   paste, sep = "\n")
    # write this file list to file
    writeLines(text =  paste("The following file(s) contained multiple hits for potential table headers.
                               The correct choice could not be determined so the file(s) was excluded",
                             duplicate_file_names, sep = "\n"), 
               con = paste0(script_output_dir, "/multiple_header_hits.txt"))
    # and remove the file_data indicies from correct_hits that correspond to files matched multiple times
    correct_hits <- correct_hits[!(file_data$file[correct_hits] %in% duplicate_file_names)]
  }
  
  correct_hits
}

create_table <- function(file_data, correct_hits, target, script_output_directory){
  if (length(correct_hits) == 0){return("No files were interpreted correctly")}
  table_assembly(file_data, correct_hits, target, 1, script_output_directory)
  
  if (length(correct_hits) > 1){
    for (i in 2:length(correct_hits)){
      table_assembly(file_data, correct_hits, target, i, script_output_directory)
      
    }
  }

}
table_assembly <- function(file_data, correct_hits, target, i, script_output_dir){
  
  table <- read_csv(file_data$file[correct_hits[i]], col_names = TRUE, 
                    col_types = cols(.default = "c"),
                    skip = file_data$skip[correct_hits[i]], skip_empty_rows = TRUE)
  colnames(table) <- gsub(pattern = target, replacement = "snow_depth",
                          x = colnames(table), ignore.case = TRUE)
  selected_columns <- table %>% select(thing, snow_depth) %>% mutate(file = file_data$file[correct_hits[i]])
  unused_columns <- colnames(table)[!(colnames(table) %in% c("thing", "snow_depth"))]
  if (length(unused_columns) > 0){
  if (file.exists(paste0(script_output_dir, "/unused_columns.csv"))){
    previous_columns <- read.csv(paste0(script_output_dir, "/unused_columns.csv"))
    unused_columns <- unused_columns[!(unused_columns %in% previous_columns$headers)]
    column_table <- tibble(headers = unused_columns, file = file_data$file[i])
    if (length(column_table) > 0){
     write_csv(column_table, file = paste0(script_output_dir, "/unused_columns.csv"), append = TRUE)
    }
  } else {
    column_table <- tibble(headers = unused_columns, file = file_data$file[i])
    write_csv(column_table, file = paste0(script_output_dir, "/unused_columns.csv"))
  }
  }
  if (length(selected_columns) == 3){
    if (file.exists(paste0(script_output_dir, "/compiled_data.csv"))){
    write_csv(selected_columns, paste0(script_output_dir, "/compiled_data.csv"), append = TRUE)
    } else {write_csv(selected_columns, paste0(script_output_dir, "/compiled_data.csv"))}
  } else if (file.exists(paste0(script_output_dir, "/tabulation_errors.txt"))) {
    write_lines(file_data$file[i], file = paste0(script_output_dir, 
                                                 "/tabulation_errors.txt"),
                                                 sep = "\n", append = TRUE)
  } else {
    writeLines(paste("Error in selecting the right columns while trying to tabulate the following files:",
               file_data$file[correct_hits[i]], sep = "|"),
               con =paste0(script_output_dir, "/tabulation_errors.txt"), sep = "\n")
  }
}

do_analysis <- function(){
  file_dir <- "/home/ross/Desktop/mock_dir/" #main directory containing all data
  # The function writes a txt file containing the results from the grep search
  # This file needs to be stored somewhere outwith the main directory above or it
  # will find its own results
  
  script_output_dir <- "/home/ross/Documents/Alex_Fjord/script_output_files"
  
  snow_depth_synonums <- c("(^|,)sno?w.?de?pth(,|$)", "(^|,)s.d(,|$)")
  
  target <- paste(snow_depth_synonums, sep = "", collapse = "|")
  system_target <- paste0("'", target, "'")
  
  file_data <- get_file_data(system_target, file_dir, script_output_dir)
  correct_hits <- determine_correct_hit(file_data, script_output_dir)
  create_table(file_data, correct_hits, target, script_output_dir)
  }
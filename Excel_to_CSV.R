#!/bin/env Rscript

library(readxl)
library(stringr)
library(tidyverse)

print("running excel_to_csv.R")

#pathToXL is a string of the path to a given .xlsx file
#inputDirectory is a 
excel_to_csv <- function(pathToXL, inputDirectory){ #converts xlsx path to a path in a directory called "duplicateCSV" with the same subdirectories 
  sheets <- FALSE # Some files are corrupt. the try function attempts to read the file. 
  # If it fails then sheets will be unchanged and remain as FALSE, then get caught by the if statment which gives a warning message and prevents the rest of the function running 
  try({sheets <- excel_sheets(pathToXL)}) #return value is a dataframe of all the sheetnames for a given workbook
  if (length(sheets) == 1){
  if (sheets == FALSE){
    warning(paste("unable to read file", pathToXL))
      return
  }
  }
  sheetCount <- length(sheets)
  
  
  
  for(i in 1:sheetCount){
    print(paste(pathToXL, "sheet: ", i))
    myData <- FALSE # Some files are corrupt. the try function attempts to read the file. 
    # If it fails then myData will be unchanged and remain as FALSE, then get caught by the if statment which gives a warning message and prevents the rest of the function running 
    try({myData <- read_excel(path = pathToXL, sheet = i, col_names = FALSE)}) #assigns Excel sheet as dataframe w/o column names; can change this depending on what the datasets actually contain
    
    if (length(myData) != 0){
      if (!is.data.frame(myData)){
        write(paste(pathToXL, "returned: ", myData, "on read sheet: ", i, sep = "\n"),
              file = "returned_NA.txt", append = TRUE, sep = " ")
        myData <- vector("integer")
      }
      }
     
    
    if (length(myData) > 0){
      sheets[i] <- gsub("[^A-Za-z0-9]", " ", sheets[i])
      sheets[i] <- gsub(" ", "_",sheets[i])
      
    filename <- paste0(gsub(pattern = ".xls?x", basename(pathToXL), replacement = ""), "_", sheets[i]) #removes .xlsx from filename and adds sheet name to filename
    fullPath <- duplicate_directory(pathToXL, inputDirectory) #this object is a text string with the full file path in it
    directory <- dirname(fullPath)
   
     if(dir.exists(directory) == FALSE){ #creates the duplicate directory if one doesn't already exist
      dir.create(directory, recursive = TRUE)
     }
    
    write.csv(myData, file = paste0(directory, "/", filename, ".csv" )) #CSV made
    }
    }
}

duplicate_directory <- function(inputPath, parentDirectory){ #output the duplicate directory for the csv
  duplicate_as_excel <- gsub(pattern = parentDirectory, replacement = paste0(parentDirectory, "/duplicateCSV"), inputPath) ###CHANGE NEW DIRECTORY NAME: this saves file path for duplicate excel file
  duplicate_as_csv <- gsub(pattern = "xls?x", replacement = "csv", duplicate_as_excel)
  return(
    paste(
      duplicate_as_csv
    )
  )
} #essentially saves an identical directory of the file without disturbing original directory

go_through_directories <- function(parentDirectory){
  
  if (!file.exists(paste0(parentDirectory, "/files.txt"))){
    create_file_list(parentDirectory)
  }
  file <- read_lines(paste0(parentDirectory, "/files.txt"), n_max = 1)
  while (!is.na(file)){
    excel_to_csv(file, parentDirectory)
    remaining_files <- read_lines(paste0(parentDirectory, "/files.txt"), skip = 1)
    write_lines(remaining_files, paste0(parentDirectory, "/files.txt"), sep = "\n")
    file <- remaining_files[1]
  }
}

create_file_list <- function(parentDirectory){
  files <- list.files(parentDirectory, pattern = ".xls?x", recursive = TRUE,
                      full.names = TRUE, all.files = FALSE, include.dirs = TRUE)
  write_lines(files, paste0(parentDirectory, "/files.txt"),sep = "\n")
}
parentDirectory <- getwd()
go_through_directories(parentDirectory) ###################NOTE: DEFINE PARENT DIRECTOR AS "/arc/project/st-ghenry-1/")
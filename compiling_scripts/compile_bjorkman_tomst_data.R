library(tundra)
library(tidyverse)

# Directory the bjorkman google drive was extracted to
bjorkman_dir <- "/home/ross/Documents/google_drive_data/"
# Dir that data shall be written to
output_dir <- "/home/ross/Desktop/bjorkman_tomst/"
compile_bjorkman_tomst <- function(bjorkman_dir, output_dir){
    # create output_dir if it doesn't already exist
    if (!dir.exists(output_dir)){
        dir.create(output_dir, recursive = TRUE)
    }
    # load tomst id look up table for finding site info
    tomst_lookup <- load_file(paste0(bjorkman_dir,
                                     "/Latnjajaure_Mats_Björkman/Tomst loggers 2020/2020-Tomst_loggers.xlsx"))
    # list all the 2020 data files
    tomst_2020_files <- list.files(paste0(bjorkman_dir, "/Latnjajaure_Mats_Björkman/Tomst loggers 2020/"),
                                   pattern = "data",
                                   full.names = TRUE)
    # list all the 2021 data files
    tomst_2021_files <- list.files(paste0(bjorkman_dir, "/Latnjajaure_Mats_Björkman/Tomst loggers 2021/"),
                                   pattern = "data",
                                   full.names = TRUE,
                                   recursive = TRUE)
    # extract all the 2020 data files
    compiled_2020 <- lapply(tomst_2020_files,
                            extract_data,
                            output_dir = output_dir,
                            tomst_lookup = tomst_lookup)
    # extract all the 2021 data files
    compiled_2021 <- lapply(tomst_2021_files,
                            extract_data,
                            output_dir = output_dir,
                            tomst_lookup = "file_path")
    # combine all data
    compiled_data <- append(compiled_2020, compiled_2021)
    compiled_data <- plyr::rbind.fill(compiled_data)

    # Write data to file
    write_csv(compiled_data, paste0(output_dir, "/compiled_bjorkman_tomst_data 2020-2021.csv"))
    compiled_data
}

extract_data <- function(file_path, output_dir, tomst_lookup){
    # load file
    dataframe <- load_file(file_path, sep = ";")
    # The known names for the columns
    col_names <- c("index",
                   "date_time_UTC",
                   "time_zone",
                   "Temp_1",
                   "Temp_2",
                   "Temp_3",
                   "soil_moisture",
                   "shake",
                   "error_if_1")
    # name the columns if the dataframe has 9 cols, include the name unknown if there is
    # one additional column, else return an error
    if (ncol(dataframe) == 9){
        names(dataframe) <- col_names
    } else if (ncol(dataframe) == 10){
        names(dataframe) <- c(col_names, "unknown")
    } else {
        stop(paste(file_path, "does not contain the appropriate number of columns"))
    }
    # extract tomst id from file_path and add file path to dataframe
    dataframe <- dataframe %>% mutate(file = file_path,
                                      tomst_id = tomst_id_extractor(file,
                                                                    unique = FALSE,
                                                                    as_null = FALSE)
    )
    # Lookup plot data from the tomst_lookup table, or extract from the file_path if
    # tomst_lookup == "file_path"
    if (tomst_lookup == "file_path"){
        # assuming the files are stored in folders named in the format "site treatment plot"
        # extract the plot data
        site_name <- grep_capture("([^ /]*) [^/]*//?[^/]*$", file_path)
        treatment_type = grep_capture("[^ /]* ([^ /]*) [^/]*//?[^/]*$", file_path)
        plot_number = grep_capture("[^ /]* [^ /]* ([^/]*)//?[^/]*$", file_path)
        dataframe <- dataframe %>% mutate(site = site_name,
                                          treatment = treatment_type,
                                          plot = plot_number
        )
    } else {
        # lookup plot data using tomst id
        plot_data <- lookup(key_col_name = "logger",
                            key = unique(dataframe$tomst_id),
                            lookup_table = tomst_lookup)
        # add the plot data to the dataframe
        dataframe <- dataframe %>% mutate(site = plot_data$community,
                                          treatment = plot_data$treatment,
                                          plot = plot_data$number)
    }

    dataframe

}

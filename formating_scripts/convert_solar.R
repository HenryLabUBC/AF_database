convert_solar <- function(input_dir, output_dir, convert_to = "W"){
  file_list <- list.files(input_dir, ".*\\.csv", full.names = TRUE)
  if (convert_to == "W"){
    convert_from <- "KW"
    factor <- 1000
  } else {
    convert_from <- "W"
    factor <- 0.001
  }
  for (file in file_list){
    dataframe <- load_file(file)
    file_name <- str_match(file, "/[^/]*\\.")
    is.solar <- solar_identifier(dataframe,
                                 file_path = file,
                                 output_dir = input_dir,
                                 W_or_KW = convert_from)
    solar_indicies <- which(is.solar, arr.ind = TRUE)
    for (index in solar_indicies){
      col_name <- names(dataframe)[index]
      dataframe <- dataframe %>%
        mutate("{col_name}" := .data[[col_name]]*factor)
    }
    write_csv(dataframe, file = paste0(output_dir, "/", file_name, "_converted.csv"))
  }
}
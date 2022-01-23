library(tundra)
library(tidyverse)

# The root directory for the Alex Database
database_dir <- "/media/ross/external/Copying_files/archive/duplicateCSV/AF_database/"
# The directory where all output from this script will be written
output_dir <- "/home/ross/Desktop/compiled_point_frame"
# the file containing the currently most up to date compiled data
old_compilation_file <- "/media/ross/external/Copying_files/archive/duplicateCSV/AF_database/Entered_data/Point_Frame_Data/Point_Frame_Data_AllYears_AllSites_(1995-2010)_PlantAbundance.csv"


#' A function to collect and compile identified point frame data, including associated meta
#' data, standardise the terms used, move data to the correct columns, and combine with
#' the previously compiled data.
compile_point_frame <- function(database_dir, output_dir, old_compilation_file){
  # If the output directory does not exist, create it
  if (!dir.exists(output_dir)){
    dir.create(output_dir, recursive = TRUE)
  }
  # Create a list of all the files to be compiled. Currently this is all relevant data from
  # 2015 and 2019
  file_list <- get_file_list(database_dir)
  # Load each file in the file list, standardise their column names, acquire meta data where 
  # necessary, fill data throughout columns when only the first row had been entered.
  data_list <- lapply(file_list, extract_data, output_dir = output_dir)
  # Combine all compiled data into one dataframe
  compiled_data <- do.call(plyr::rbind.fill, data_list)
  # remove unneeded columns and rename .plant_species as species
  compiled_data <- compiled_data %>% select(!any_of(c("species",
                                                      "is_species",
                                                      "is_other",
                                                      "spp",
                                                      "spp_._1",
                                                      "x1",
                                                      "x2",
                                                      "extra_sp")
  )
  ) %>%
    rename(species = .plant_species)
  # load previously compiled data
  previous_compilation <- load_file(old_compilation_file)
  # Format previous compilation to match format of the newly compiled data. This function is based
  # on the 1995-2010 compilation and may need amended for other files
  previous_compilation <- format_previous_compilation(previous_compilation)
  # combine previous compilation with the new
  compiled_data <- plyr::rbind.fill(previous_compilation, compiled_data)
  # Create a multi-level named list of all the synonyms for every term found in the columns
  # species, status, tissue, state, and observer
  syn_lists <- create_syn_lists()
  # Standardise all of the terms used in columns species, status, tissue, and state
  compiled_data <- standardise_attributes(compiled_data, syn_lists)
  # Move any data in the aforementioned columns to the correct column, if neccesary, including
  # splitting terms like "live leaf" into "live" and "leaf"
  compiled_data <- data_to_right_cols(compiled_data, syn_lists)
  
  # Clean the data found in the additional species column such that all additional species found in a
  # given plot are present in one string separated by "__", and that all species names are standardised
  # to the same terms in the species column
  compiled_data <- compiled_data %>% group_by(additional_species) %>%
    mutate(additional_species = clean_attribute(additional_species, list(), syn_lists$species_syn_list))
  # Standardise plot data and create a plot_id
  compiled_data <- standardise_plots(compiled_data, output_dir, "")
  compiled_data
  
}
# A short function to format the old compiled data match the format of the new data. It is written to
# format the 1995-2010 data and may need rewritten to format other compilations in the future,
# although this step will be able to be skipped entirely in the future if the previous_compilation
# was created by this script
format_previous_compilation <- function(dataframe){
  # Rename columns to match the new data
  dataframe <- dataframe %>% rename(plot_code = plot, location = site, site = subsite, treatment = trtmt, species = spp)
  # Extract the plot number from the plot_code
  dataframe <- dataframe %>% mutate(plot = sub(".*\\.([^\\.]*$)", "\\1", plot_code))
  dataframe
}

# A function which creates a list of all the point frame data files in the csv AF database from 2015 and 2019
get_file_list <- function(database_dir){
  # List all the files in the species specific 2015 point frame data
  all_files_2015_cass <- list.files(paste0(database_dir, "AF_field_data_by_year/2015/Point_Frame_Data/Cassiope_Site/"),
                                    recursive = TRUE,
                                    include.dirs = FALSE,
                                    full.names = TRUE)
  all_files_2015_dry <- list.files(paste0(database_dir, "AF_field_data_by_year/2015/Point_Frame_Data/Dryas_Site/"),
                                   recursive = TRUE,
                                   include.dirs = FALSE,
                                   full.names = TRUE)
  all_files_2015_will <- list.files(paste0(database_dir, "AF_field_data_by_year/2015/Point_Frame_Data/Willow_Site/"),
                                    recursive = TRUE,
                                    include.dirs = FALSE,
                                    full.names = TRUE)
  # Combine all species together
  all_files_2015 <- c(all_files_2015_will, all_files_2015_dry, all_files_2015_cass)
  # Remove all of the meta data files such as species lists and status keys
  data_files_2015 <- all_files_2015[!grepl("CASTET|Template|status|LF_AND_FLO|SPP", all_files_2015, ignore.case = TRUE)]
  
  # List all the 2019 point frame data files
  all_files_2019 <- list.files(paste0(database_dir, "AF_field_data_by_year/2019/Point_frame/"),
                               recursive = TRUE,
                               include.dirs = FALSE,
                               full.names = TRUE)
  # Remove all of the meta data files such as species lists and status keys
  data_files_2019 <- all_files_2019[!grepl("codes|meta|Export", all_files_2019, ignore.case = TRUE)]
  
  # Combine the different years
  all_data_files <- c(data_files_2015, data_files_2019)
  # Remove any files manually found to not contain useful data
  all_data_files <- all_data_files[
    !grepl("AF_field_data_by_year/2019/Point_frame//?PF_SITE_TRMT_Plot_2019_PF_data.csv", all_data_files)
    ]
  all_data_files
}

# A function which loads a file, standardises it's column names, acquires meta data where 
# necessary, and fills data throughout columns when only the first row had been entered.
extract_data <- function(file_path, output_dir){
  # Print file path so if the script crashes the problematic file is easily identified
  print(file_path)
  # Load the file
  dataframe <- load_file(file_path)
  # Exclude small files as they are not point frame data
  if (nrow(dataframe) < 10 | ncol(dataframe) < 5){
    return()
  }
  # Determine which columns are "species" and "other species" and name them ".plant_species"
  # and "additional_species" to ensure they do not get confused with other named columns from
  # the original data
  dataframe <- find_other_species_col(dataframe)
  # Standardise all column names
  dataframe <- standardise_col_names(dataframe)
  
  
  # If height is present in cm, convert to mm
  dataframe <- dataframe %>% mutate(file = file_path)
  if ("height_cm" %in% names(dataframe)){
    dataframe <- mutate(dataframe, canopy_height_mm = 10*as.numeric(height_cm))
    dataframe <- select(dataframe, !height_cm)
  }
  # Obtain any meta data available for the file. Data sometimes available in meta data include
  # year, day, observer, height_a-d
  dataframe <- obtain_meta_data(dataframe, file_path)
  
  # Some columns only have their respective data recorded on the first cell, and thus the data
  # needs filled into the rest of the cells
  fill_col_regex <- c("^site$", "^plot$", "height_[a-d]", "observer", "treatment")
  # Identify which columns that potentially need filling are present in the dataframe
  fill_cols <- unlist(lapply(fill_col_regex, grep, x = names(dataframe), ignore.case = TRUE, value = TRUE))
  # Continue with filling if at least one of the columns are present
  if (length(fill_cols) > 0){
    # for each column to be filled, fill it with entry one of the column if the remainder of that
    # column is filled with either Na or empty strings
    for (col in fill_cols){
      
      if ((length(unique(dataframe[[col]][2:nrow(dataframe)])) == 1 &
           is.na(unique(dataframe[[col]][2:nrow(dataframe)]))) | (
             length(unique(dataframe[[col]][2:nrow(dataframe)])) == 1 &
             unique(dataframe[[col]][2:nrow(dataframe)]) == "")
      ){
        dataframe <- dataframe %>% mutate("{col}" := .data[[col]][1])
      }
    }
  }
  # The canopy height has only been recorded for the first hit of each point and time.
  # Fill the data into all the other rows for that time and place.
  dataframe <- dataframe %>% group_by(site, plot, year, x, y) %>%
    mutate(canopy_height_mm = max(as.numeric(cur_data()[["canopy_height_mm"]]), na.rm = TRUE)) %>%
    ungroup() %>% 
    mutate(canopy_height_mm = ifelse(is.infinite(canopy_height_mm), NA, canopy_height_mm))
  
  #' The additional species are generally listed in the first few rows individually. Combine these
  #' strings, if present, into one with "__" as separator, and fill the whole column
  if ("additional_species" %in% names(dataframe)){
    dataframe <- dataframe %>% group_by(year, site, plot) %>%
      mutate(additional_species = paste0(additional_species[!is.na(additional_species) & additional_species != ""], collapse = "__")) %>%
      ungroup()
  }
  #' The file "AF_field_data_by_year/2015/Point_Frame_Data/Cassiope_Site//PF_Cass_Site_C8_cover_PF_CASS_.csv" contains the warning
  #' "ABOVE THIS ROW ALL LICHENFL ARE LICHENFR. CORRECT THIS AT COMPUTER!". This appears to already
  #' have been addressed as there are no LICHENFL entries above this point. remove this warning to
  #' prevent future confusion
  if (grepl("AF_field_data_by_year/2015/Point_Frame_Data/Cassiope_Site//?PF_Cass_Site_C8_cover_PF_CASS_.csv", file_path)){
    dataframe <- dataframe %>% 
      mutate(state = ifelse(state == "ABOVE THIS ROW ALL LICHENFL ARE LICHENFR. CORRECT THIS AT COMPUTER!",
                            NA, state))
  }
  dataframe
}

# A function to standardise the column names of a dataframe
standardise_col_names <- function(dataframe){
  # Create a list of all the different synonyms found for column names. All names will
  # be standardised to the first in their list
  syn_list <- list(
    a_height_syn <- c("height_a", "a"),
    b_height_syn <- c("height_b", "b"),
    c_height_syn <- c("height_c", "c"),
    d_height_syn <- c("height_d", "d"),
    treatment_syn <- c("treatment", "trtmt"),
    order_syn <- c("hit_order", "order", "hit order"),
    cannopy_height_syn <- c("canopy_height_mm", "height_mm")
  )
  
  # For each list in the syn_list, if a column name from the dataframe matches a name
  # in the syn_list, ensure the name in the dataframe matches the first entry in the list
  for (col_syns in syn_list){
    for (i in seq_along(names(dataframe))){
      if (names(dataframe)[i] %in% col_syns){
        names(dataframe)[i] <- col_syns[1]
      }
    }
  }
  dataframe
}

# Not all relevant data can be found in the dataframes. Some files have data such as year
# or day at the top of the file before the column headers, and the 2019 has some of the 
# data in separate files. Store all this data (Year, Day, Observer, and heights a-d) in the
# dataframe.
obtain_meta_data <- function(dataframe, file_path){
  # Find the year, from within the file if present, else from the file_path
  dataframe <- fill_parameter(dataframe,
                              extractor_function = year_extractor,
                              output_dir = output_dir,
                              file_path = file_path,
                              col_name = "year",
                              parameter_names = c("year")
  )
  # The 2019 files have meta data in a separate file. Change file_path to reflect this
  # Also, the 2019 files place a,b,c,d as meta data while 2015 does not, thus extract the
  # 2019 a,b,c,d data if year is 2019
  if ("year" %in% names(dataframe)){
    if (dataframe$year == "2019"){
      # Create a file path to the meta data file
      file_path <- sub("PF_data.csv", "Meta_data.csv", file_path)
      # extract data a-d and place in columns height_a - height_d
      col_names <- c("height_a", "height_b", "height_c", "height_d")
      # A list of the names that the fill_parameter function will search the meta data file for.
      # Fill_parameter uses readlines to read the data file and so any character string will
      # be found encapsulated by inverted commas. Hence, by encapsulating A-D, we ensure that a
      # hit is only found if the letter is the only entry in a column, as opposed to being part of
      # a word or sentence.
      parameter_names <- c('"A"', '"B"', '"C"', '"D"')
      # The label is required by the adjacent_extractor function to extract whatever data is found
      # adjacent to the label.
      labels <- c("A", "B", "C", "D")
      for (i in seq_along(col_names)){
        dataframe <- fill_parameter(dataframe,
                                    extractor_function = adjacent_extractor,
                                    output_dir = output_dir,
                                    file_path = file_path,
                                    col_name = col_names[i],
                                    parameter_names = parameter_names[i],
                                    meta_only = TRUE,
                                    label = labels[i],
                                    full_existing_check = FALSE
        )
      }
    }
  }
  # extract the day from meta data
  dataframe <- fill_parameter(dataframe,
                              extractor_function = DOY_extractor,
                              output_dir = output_dir,
                              file_path = file_path,
                              col_name = "day",
                              parameter_names = c("date", "D.?O.?Y.?"),
                              three_digits = TRUE
  )
  # extract observer from meta data
  dataframe <- fill_parameter(dataframe,
                              extractor_function = adjacent_extractor,
                              output_dir = output_dir,
                              file_path = file_path,
                              col_name = "observer",
                              parameter_names = c("observers?:?"),
                              meta_only = TRUE,
                              label = "observers?:?",
                              full_existing_check = FALSE
  )
  dataframe
}


create_syn_lists <- function(){
  syn_lists <- list(
    status_syn_list = list(
      atached_dead_syns = c("attached_dead", "ATDEAD", "AD", "XXXATTACHEDDEAD"),
      dead_syns = c("dead", "black", "XXXDEADPLANT"),
      live_syns = c("live", "L", "li", "LIVE1"),
      standing_dead_syns = c("standing_dead", "XSTDEAD", "STDEAD", "st dead",
                             "STEAD", "SD", "STANDINGDEAD"),
      unknown_syns = c("SC", "ST"),
      live_leaf_syns = c("live__leaf", "live lf")
    ),
    tissue_syn_list = list(
      dlstem_syns = c("dead_leaf-stem", "DLSTEM"),
      llstem_syns = c("leaf-stem-leaf", "llstem"),
      flower_syns = c("flower", "FLO", "CAT", "CATKIN", "CATKIN F", "CATKINF", "FLOW", "CAP", "FL"),
      flower_stem_syns = c("flower-stem", "flost", "flostem", "flstem", "flstm", "flst"),
      leaf_syns = c("leaf", "lf", "L", "LV", "bud"),
      stem_syns = c("stem", "st", "ST EM", "stem A", "branch"),
      unknown = c("C", "LFC", "SP", "SPO", "SPORO", "STO", "SPOR", "FLOB"),
      flower_male_syns = c("flower__male", "FL-m"),
      flower_female_syns = c("flower__female", "FLO-F"),
      dead_leaf_syns = c("dead__leaf", "DL", "LD"),
      leaf_eaten_syns = c("leaf__eaten", "lf (eaten)"),
      dead_moss_syns = c("dead__moss", "dead moss patch")
    ),
    leaf_state_syn_list = list(
      eaten_syns = c("eaten", "E"),
      rust_syns = c("rust", "R"),
      mites_syns = c("mites", "M", "Galls", "G"),
      fungus_syns = c("fungus", "F"),
      eaten_rust_syns = c("eaten__rust", "E, R", "R, E", "E,R", "R,E", "E R", "R E"),
      eaten_mites_syns = c("eaten__mites", "E, M", "M, E", "E,M", "M,E", "E M", "M E",
                           "E, G", "G, E", "E,G", "G,E", "E G", "G E"),
      eaten_fungus_syns = c("eaten__fungus", "E, F", "F, E", "E,F", "F,E", "E F", "F E"),
      diseased_leaf_syns = c("disease")
    ),
    flower_state_syn_list = list(
      male_syns = c("male", "M"),
      female_syns = c("female", "FEM", "F", "femal"),
      mature_syns = c("mature", "MAT"),
      senescent_syns = c("senescent", "SEN")
    ),
    species_syn_list = list(
      alater_syns = c("ALATER"),
      arclat_syns = c("ARCLAT"),
      brapur_syns = c("BRAPUR"),
      caramb_syns = c("CARAMB"),
      caraqu_syns = c("CARAQU"),
      caratr_syns = c("CARATR"),
      carbel_syns = c("CARBEL"),
      carmem_syns = c("CARMEM"),
      carmis_syns = c("CARMIS", "CASMIS"),
      carsci_syns = c("CARSCI"),
      carsta_syns = c("CARSTA"),
      carspp_syns = c("CARSPP", "XXXCARSPP"),
      castet_syns = c("CASTET", "CASSTET", "CASTETE"),
      ceralp_syns = c("CERALP"),
      dracin_syns = c("DRACIN"),
      dralac_syns = c("DRALAC"),
      draspp_syns = c("DRASPP", "DRABA SP1", "DRALAC?", "DRABASP","DRABA SP (sp1). 'CASSIOPE DRABA': DRABA CERNUA??!",
                      "XXXDRASPP"),
      dryint_syns = c("DRYINT", "DRY", "DRYINT L"),
      eriang_syns = c("ERIANG"),
      erisch_syns = c("ERISCH"),
      eritri_syns = c("ERITRI"),
      excrament_syns = c("excrament", "poo", "poop", "XXXDUNG"),
      equarv_syns = c("EQUARV"),
      equavr_syns = c("EQUAVR"),
      
      feather_syns = c("feather"),
      fesbra_syns = c("FESBRA", "FEBRA", "FESRAD"),
      feshyp_syns = c("FESHYP"),
      grass_syns = c("grass", "XXXGRASS", "XXXGRASSD"),
      hobo_syns = c("hobo", "hoboshelter"),
      hole_syns = c("hole", "XXXHOLE"),
      junbig_syns = c("JUNBIG"),
      kobmyo_syns = c("KOBMYO"),
      kobsim_syns = c("KOBSIM"),
      lichencru_syns = c("LICHENCRU", "lichen C", "LICHENCR", "LICHENC", "LICHEC", "LICHNC",
                         "XXXCRUSTOSE2000FERTBLACK", "XXXCRUSTOSE2000FERTWHITE"),
      lichenfol_syns = c("LICHENFOL", "LICHENFL", "LICHENFO", "LICHFO"),
      lichenfru_syns = c("LICHENFRU", "LICHENFR", "LICHFR", "LICHNFR"),
      lichenspp_syns = c("LICHENSPP", "XXXLICHEN"),
      litter_syns = c("litter", "litter J", "litter O", "llitter", "L", "lit",
                      "lit+", "lit +", "XXXLITTER"),
      luzspp_syns = c("LUZSPP", "LUZ"),
      luzarc_syns = c("LUZARC"),
      luzcon_syns = c("LUZCON"),
      lycspp_syns = c("LYCSPP", "XXXLYCSPP"),
      moss_syns = c("moss", "m oss", "XXXMOSS"),
      melape_syns = c("MELAPE"),
      melaff_syns = c("MELAFF"),
      minrub_syns = c("MINRUB"),
      mushroom_syns = c("mushroom", "fungus", "puffball", "XXXFUNGUS"),
      nostoc_syns = c("NOSTOC"),
      oxydig_syns = c("OXYDIG", "OXIDIG"),
      pan_trap_syns = c("pan_trap", "XXXPAN TRAP"),
      pollen_trap_syns = c("pollen_trap", "XXXPOLLEN TRAP"),
      paprad_syns = c("PAPRAD", "PAPAD", "PARRAD", "poppy", "POPRAD"),
      pedcap_syns = c("PEDCAP"),
      pedhir_syns = c("PEDHIR"),
      poaarc_syns = c("POAARC", "POAACT"),
      poagla_syns = c("POAGLA"),
      polviv_syns = c("POLVIV"),
      rock_syns = c("rock", "roche", "XXXGROUNDROCK"),
      salarc_syns = c("SALARC", "sal arc", "SALACT", "SALARC L"),
      saxcer_syns = c("SAXCER"),
      saxniv_syns = c("SAXNIV"),
      saxopp_syns = c("SAXOPP"),
      silaca_syns = c("SILACA"),
      soil_syns = c("soil", "bare", "BGRN", "BARGND", "BGND", "sand", "XXXGROUNDBARE"),
      stelon_syns = c("STELON"),
      tag_syns = c("tag", "stick", "wire", "flag", "XXXMARKER"),
      vaculi_syns = c("VACULI"),
      water_syns = c("water", "XXXWATERSTANDING"),
      whitecrust_syns = c("whitecrust", "WCRU"),
      wood_syns = c("wood"),
      unknwown = c("STETSON", "MO", "MORE", "SP1", "ARCTIC", "BCRU", "WOOLYBEAR", 
                   "XXXBLACKALGAE", "XXXAGARICUS", "XXXSDUNK", "XXXSEEDCASSIOPE96DICOT",
                   "XXXSEEDCASSIOPE96MONOCOT", "XXXUNK")
    )
  )
  syn_lists <- c(syn_lists, state_syn_list = list(c(syn_lists$leaf_state_syn_list, syn_lists$flower_state_syn_list)))
  list_types <- names(syn_lists)
  for (i in seq_along(list_types)){
    type_list <- list()
    for (var in syn_lists[[list_types[i]]]){
      type_list <- append(type_list, var)
    }
    syn_lists <- c(syn_lists, list(type_list))
    name <- paste0("all_", list_types[i])
    name <- sub("_syn_list", "", name)
    names(syn_lists)[length(list_types) + i] <- name
  }
  all_state_syns <- c(syn_lists$leaf_state_syn_list, syn_lists$flower_state_syn_list)
  all_state_list <- list()
  for (var in all_state_syns){
    all_state_list <- append(all_state_list, var)
  }
  syn_lists <- c(syn_lists, all_state = list(all_state_list))
  syn_lists
}

find_other_species_col <- function(dataframe){
  syn_lists <- create_syn_lists()
  all_other_list <- with(syn_lists, c(all_status, all_tissue, all_leaf_state, all_flower_state))
  for (col in names(dataframe)){
    if (col %in% c("tissue", "status", "state")) {next}
    dataframe <- dataframe %>% rowwise() %>%  mutate(
      is_species = ifelse(contains_attribute(.data[[col]], syn_lists$all_species),
                          TRUE, FALSE),
      is_other = ifelse(.data[[col]] %in% all_other_list, TRUE, FALSE)
    ) %>% ungroup()
    
    if (with(dataframe, sum(is_species) > 0 & sum(is_species) < 10 & sum(is_other) <= 2)){
      dataframe <- rename(dataframe, additional_species = .data[[col]])
      
    }
    if (sum(dataframe$is_species) > 10 & length(unique(dataframe[[col]])) > 1){
      dataframe <- rename(dataframe, .plant_species = .data[[col]])
      
    }
  }
  dataframe
}
contains_attribute <- function(string, syn_list, sep = " ?,?/? |/|_"){
  is_attribute <- sum(tolower(str_split(string, sep, simplify = TRUE)) %in% tolower(syn_list)) > 0
  is_attribute
}
#' A function which accepts a string of terms separated by "__" and returns a string 
#' containing any of the original terms that are found in the provided syn list, separated
#' by "__" if there is more than one term. This can be used to extract any term given in the
#' defined list. It excludes "L" and "Li" because these can be both attributed to "litter" and
#' "live" and so it is not possible to determine which column it should belong to.
extract_attribute <- function(string, syn_list, combine = TRUE){
  exclude_list <- c("L", "Li", "FUNGUS")
  attributes <- str_split(string, "__", simplify = TRUE)[
    tolower(str_split(string, "__", simplify = TRUE)) %in% tolower(syn_list) &
      !tolower(str_split(string, "__", simplify = TRUE)) %in% tolower(exclude_list)
  ]
  if (combine){
    attributes <- paste0(attributes, collapse = "__")
  }
  attributes
}
#' A function which takes a string of terms separated by "__" and returns a string
#' containing any of the original terms not found in the provided syn list. If the 
#' provided syn list is a list containing all the column syns not pertaining to the
#' column of interest then this function will remove all terms known to be in the wrong
#' column while leaving the correct terms as well as those which are unrecognized.
#' It excludes "L" and "Li" because these can be both attributed to "litter" and
#' "live" and so it is not possible to determine which column it should belong to.
clean_attribute <- function(string, remove_list, standardise_list){
  exclude_list <- c("L", "Li", "FUNGUS")
  attributes <- str_split(string, "__", simplify = TRUE)[(
    !tolower(str_split(string, "__", simplify = TRUE)) %in% tolower(remove_list) |
      tolower(str_split(string, "__", simplify = TRUE)) %in% tolower(exclude_list)
  ) & str_split(string, "__", simplify = TRUE) != "NA"
  ]
  for (i in seq_along(attributes)){
    for (syns in standardise_list){
      if (tolower(attributes[i]) %in% tolower(syns)){
        attributes[i] <- syns[1]
      }
    }
  }
  attributes <- unique(attributes)
  attributes <- paste0(attributes, collapse = "__")
  attributes
}

standardise_attributes <- function(dataframe, syn_lists){
  
  cols <- c("species", "status", "tissue", "state")
  for (col in cols){
    syn_list_name <- paste0(col, "_syn_list")
    for (syns in syn_lists[[syn_list_name]]){
      dataframe <- dataframe %>% mutate("{col}" := ifelse(tolower(.data[[col]]) %in% tolower(syns),
                                                          syns[1],
                                                          .data[[col]]
      )
      )
    }
  }
  dataframe
}

# Create new function which extracts the hit
data_to_right_cols <- function(dataframe, syn_lists){
  cols <- c("species", "status", "tissue", "state")
  
  for (col in cols){
    other_cols <- cols[cols != col]
    syn_col_name <- paste0("all_", col)
    for (other_col in other_cols){
      dataframe <- dataframe %>% rowwise() %>%
        mutate("{col}" := ifelse(contains_attribute(.data[[other_col]], syn_lists[[syn_col_name]], sep = "__"),
                                 paste0(.data[[col]],
                                        "__",
                                        extract_attribute(.data[[other_col]], syn_lists[[syn_col_name]])
                                 ),
                                 .data[[col]])
        )
    }
  }
  
  for (col in cols){
    syn_name <- paste0(col, "_syn_list")
    other_cols <- cols[cols != col]
    other_syn_lists <- vector("character")
    for (other_col in other_cols){
      other_syn_name <- paste0("all_", other_col)
      other_syn_lists <- c(other_syn_lists, syn_lists[[other_syn_name]])
    }
    dataframe <- dataframe %>% rowwise() %>% mutate("{col}" := clean_attribute(.data[[col]], other_syn_lists, syn_lists[[syn_name]])
    )
  }
  
  dataframe <- dataframe %>% 
    mutate(leaf_state = ifelse(state %in% syn_lists$all_leaf_state, state, NA),
           flower_state = ifelse(state %in% syn_lists$all_flower_state, state, NA),
           state = ifelse(state %in% syn_lists$all_leaf_state | 
                            state %in% syn_lists$all_flower_state, NA, state)
    )
  dataframe
}
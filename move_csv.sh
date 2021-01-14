#!/bin/bash

# A script that copies all CSV files within a given directory (recursivley) into
# a directory named duplicateCSV, contained within the original directory. If no
# such directory exists it will be created. Files already contained within the 
# directory will not be copied. The script uses relative file paths so the user
# must first navigate to the location they wish to extract files from before running
# The script stores data in text files as opposed to variables as the latter approach 
# was producing memory issues.
 
echo running move_csv.sh
if [[ ! -f new_full_file_paths.txt ]];then
	# find normal files (not directories) that are not hidden files or already in 
	# the duplicate folder, if there is one and write to file files.txt
	find -not -path './duplicateCSV*' -type f|grep -vE '.*/\.' > files.txt



	# for each file in files.txt, determine file type, issolate those of 
	# CSV type, and remove all extranious data produced by the file command
	# to leave only the full file path which is written to file
	file -f files.txt|\
	grep -E ':[ ]*CSV text' $file_types|\
	sed 's/:.*//' > full_file_paths.txt

	# The file command is unreliable at detecting CSV files, so excluding all
	# files that file determines as type CSV, find all files with .csv and .dat
	# extenstions, extract only the file paths from the output, and append these
	# paths to the full_file_paths.txt file

	file -f files.txt|\
	grep -v ":[ ]*CSV"|\
	grep -Ei "\.csv|\.dat"|\
	sed s_:.*__|\
	grep -v ".*/\." >> full_file_paths.txt

	# From the file paths, extract the file names and write to file_names.txt
	sed 's_.*/__' full_file_paths.txt > file_names.txt
	# From the full file paths (i.e path + file name), extract the path and write
	# to file_paths.txt
	sed 's_[^/]*$__' full_file_paths.txt > file_paths.txt
	# Create new file paths by replacing the initial dot of the relative old file
	# path with duplicateCSV and write new file paths to new_file_paths.txt
	sed 's_^\._duplicateCSV_' file_paths.txt > new_file_paths.txt

	# combine new file paths with file names. The paste command combines the new
	# file path with the file name seperated by a delimeter collon. The subsequent
	# sed command removes the collons. New full file paths are written to file. 
	paste -d: new_file_paths.txt file_names.txt > colon_file_paths.txt
	sed 's/://' colon_file_paths.txt > new_full_file_paths.txt

fi

# full file paths (path to old file location), new full file paths (path to new location)
# and new file paths (path without file name) are read into variables of the same names
#full_file_paths=($(cat full_file_paths.txt))
#new_full_file_paths=($(cat new_full_file_paths.txt))
#new_file_paths=($(cat new_file_paths.txt))


while (( ! $(wc -l full_file_paths.txt |sed 's/ .*//') == 0 )); do

	full_file_path=$(sed -n 1p full_file_paths.txt)
	new_full_file_path=$(sed -n 1p new_full_file_paths.txt)
	new_file_path=$(sed -n 1p new_file_paths.txt)


	if [[ ! -d  $new_file_path ]]; then
		mkdir -p $new_file_path
		cp -v $full_file_path $new_full_file_path
		exit_status=$?
		if (( ! $exit_status == 0 ));then
			echo $full_file_path >> copy_failed.txt
		fi
	else
		cp -v $full_file_path $new_full_file_path
		exit_status=$?
		if (( ! $exit_status == 0 ));then
			echo $full_file_path >> copy_failed.txt
		fi
	fi

	
	sed 1d full_file_paths.txt > temp_full_file_paths.txt
	
	mv temp_full_file_paths.txt full_file_paths.txt
	sed 1d new_full_file_paths.txt > temp_new_full_file_paths.txt
	mv temp_new_full_file_paths.txt new_full_file_paths.txt
	sed 1d new_file_paths.txt > temp_new_file_paths.txt
	mv temp_new_file_paths.txt new_file_paths.txt
done

# # range is set as the number of full file paths
# range=$((${#full_file_paths[*]}))

# # For every file in full_file_paths, if new directory doesn't yet exist, create directory and
# # place a copy of the old file in the new directory. If the directory does exist, place a copy
# # of the old file in the existing directory
# for ((i=0;i<$range;i++)); do
# 	if [[ ! -d  ${new_file_paths["$i"]} ]]; then
# 		mkdir -p ${new_file_paths["$i"]}
# 		cp -v ${full_file_paths[$i]} ${new_full_file_paths[$i]}
# 	else
# 		cp -v ${full_file_paths[$i]} ${new_full_file_paths[$i]}
# 	fi
# done

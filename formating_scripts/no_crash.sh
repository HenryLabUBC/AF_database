#!/bin/bash

exit_status=1
last_file="first"
script=$1
while (( ! $exit_status == 0)); do
	$1
	exit_status=$?
	if (( ! $exit_status == 0 )); then
		crashed_file=$(sed -n 1p $2)
		if (( wc -m $crashed_file == 0 )); then
			echo 'script failed with no files left to process' >&2
			echo 'script failed with no files left to process' >> $1"_error.txt"
			exit 1
		fi
		if [[ $crashed_file == $last_file ]]; then

			arguments=($@)
			for ((i=1; i<${#arguments[@]}; ++i));do
				echo ${arguments[$i]}
				sed 1d ${arguments[$i]} > ./remaining_files_temp.txt
				mv ./remaining_files_temp.txt ${arguments[$i]}
			done
			echo $crashed_file >> $1"_error.txt"
		fi
	fi
	last_file=$crashed_file
done

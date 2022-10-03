#!/bin/bash

set -e

code_folder=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
data_folder=$1
raw_csv_folder=$data_folder/csv/raw
cleaned_csv_folder=$data_folder/csv/cleaned

cd $raw_csv_folder
ls $raw_csv_folder
echo
read -p "Is this the right folder containing CSV files? And do you have correct activated python VENV? And are you sure the csv's contain complete data up to at most one month from today's date?" -n 1 -r

if [[ $REPLY =~ ^[Yy]$ ]]
then

    echo "Okay, running script..."
    # dedup all files in directory by shasum
    echo "Deduplicating files..."
    declare -A arr
    shopt -s globstar

    for file in **; do
      [[ -f "$file" ]] || continue
       
      read cksm _ < <(md5sum "$file")
      if ((arr[$cksm]++)); then 
        echo "removing $file"
        rm $file
      fi
    done


    echo "Processing raw files..."
    cd $code_folder
    python3 clean_CSVs.py $data_folder
    cd $code_folder
    find $cleaned_csv_folder -type f -name "citi*" -exec python3 citi.py {} \;

fi

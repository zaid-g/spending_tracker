#!/bin/bash

set -e

echo "Running..."
echo

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters. Did you pass data folder path?"
fi

code_folder=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
data_folder=$(realpath $1)
raw_csv_folder=$data_folder/raw
cleaned_csv_folder=$data_folder/cleaned
mkdir -p $data_folder
mkdir -p $raw_csv_folder
mkdir -p $cleaned_csv_folder

cd $raw_csv_folder
echo "Are all these correct?"
echo
echo "1. Is this the right folder containing raw CSV files?"
echo
echo $raw_csv_folder
echo
python3 ${code_folder}/scripts/pretty_print_raw_data_files.py $data_folder
echo
echo "2. Do you have correct activated python virtual environment?"
echo "3. Are you sure the csv's contain complete data up to at most one month from today's date?" 
echo
echo "[Y/n]  "
echo
read -p "" -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]
then

    echo "Okay, running script..."
    # dedup all files in directory by shasum
    echo "Deduplicating files... if didn't print then no duplicates found"
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


    echo
    echo
    echo "Running python script..."
    cd $code_folder
    python3 main.py $data_folder
fi

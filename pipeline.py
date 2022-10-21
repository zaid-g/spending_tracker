from copy import deepcopy
import ipdb
import re
import json
import hashlib
import datetime
import dateutil.parser
import re
import glob
import sys
import numpy as np
import pandas as pd
import os
from os import listdir
from os.path import isfile, join
from utils import *

pd.set_option("display.max_rows", 10000)


# ---------- [read csv file names and make sure no problems] ----------:


data_fol_path = sys.argv[1] + "/"
historical_categorized_csv_path = data_fol_path + "history.csv"
raw_csv_path = data_fol_path + "raw/"
cleaned_csv_path = data_fol_path + "cleaned/"
raw_csv_file_names = [f for f in listdir(raw_csv_path) if isfile(join(raw_csv_path, f))]
raw_csv_file_names = [
    file_name for file_name in raw_csv_file_names if file_name[0] != "."
]  # remove hidden raw_csv_file_names


# make sure all raw_csv_file_names have date range
for file_name in raw_csv_file_names:
    if not contains_date_range(file_name):
        raise Exception(
            f"No date range detected in file {file}. Format is \n^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}"
        )


# ---------- [rm whitespace, lowercase csv file names, reread names] ----------:

for file_name in raw_csv_file_names:
    file_path = raw_csv_path + file_name
    formatted_file_name = "".join(file_name.split()).lower()
    formatted_file_path = raw_csv_path + formatted_file_name
    os.rename(file_path, formatted_file_path)

raw_csv_file_names = [f for f in listdir(raw_csv_path) if isfile(join(raw_csv_path, f))]
raw_csv_file_names = [
    file_name for file_name in raw_csv_file_names if file_name[0] != "."
]  # remove hidden raw_csv_file_names


# ---------- [clean files] ----------:


for file_name in raw_csv_file_names:
    file_path = raw_csv_path + file_name
    detect_file_source(file_path)(file_path, file_name, cleaned_csv_path)

# ---------- [merge into final csv] ----------:

cleaned_csv_file_names = glob.glob(os.path.join(cleaned_csv_path, "*.csv"))

li = []
for filename in cleaned_csv_file_names:
    df = pd.read_csv(filename, index_col=None, header=0, parse_dates=["datetime"])
    li.append(df)

df = pd.concat(li, axis=0, ignore_index=True)

df = df[
    [
        "id",
        "datetime",
        "amount",
        "source",
        "preselected_category",
        "note",
    ]
]

assert len(df) == len(
    df.id.value_counts()
), "Error: found duplicate ID(s) in cleaned files"


# first read entire df including written history
if os.path.isfile(historical_categorized_csv_path) is False:
    hist_df = pd.DataFrame(
        columns=[
            "id",
            "datetime",
            "amount",
            "source",
            "preselected_category",
            "note",
            "category",
            "pattern",
        ]
    )
else:
    hist_df = pd.read_csv(historical_categorized_csv_path, parse_dates=["datetime"])

# assert all patterns in historical file indeed do match text of that transaction
hist_df.apply(
    lambda row: make_sure_pattern_matches_text(row["pattern"], row["note"]), axis=1
)

# store all possible categories and patterns in variable
(
    pattern_category_map_list,
    pattern_category_map_dict,
    all_categories,
    all_patterns,
) = extract_patterns_categories_from_history(hist_df)

# make sure no pattern maps to more than one category
make_sure_no_pattern_maps_to_more_than_one_category(pattern_category_map_list)


# assert all categories are valid in historical file
hist_df["category"].apply(lambda category: make_sure_category_dtype(category))

# make sure no ids are duplicated
assert len(hist_df) == len(
    hist_df.id.value_counts()
), "Error: found duplicate IDs in historical file"

# make sure all historical transactions are accounted for in cleaned_csvs
assert set(hist_df.id.values).issubset(
    df.id.values
), "Error: not all historical transactions accounted for in cleaned or raw csvs"

# apply patterns on new data (df)
df["pattern"] = df["note"].apply(
    lambda text: get_matched_pattern(text, pattern_category_map_dict)
)
df["category"] = df["pattern"].apply(
    lambda pattern: get_category_from_pattern(pattern, pattern_category_map_dict)
)


# update df to include history
df = df[~df.id.isin(hist_df.id.values)]
df = pd.concat([df, hist_df], axis=0, ignore_index=True)
df = df.sort_values("datetime", ascending=False, ignore_index=True)
df = df.sort_index(ascending=False)
# make sure no ids are duplicated
assert len(hist_df) == len(
    hist_df.id.value_counts()
), "Error: found duplicate IDs in concatenated history + recent file (really weird if you get this error)"

# time to ask user to confirm or override
# sort categories and patterns for visual display
while True:

    all_categories.sort()
    all_patterns.sort()

    print()
    print(
        df[
            [
                "note",
                "category",
                "pattern",
                "amount",
                "datetime",
                "source",
                "preselected_category",
            ]
        ]
    )

    while True:
        try:
            transaction_index = int(
                input(
                    "\nSelect row you would like to categorize.\nEnter -1 if this looks good.\nEnter -2 for breakpoint.\nEnter -3 to quit without saving.\n"
                )
            )
            if transaction_index >= 0:
                df.loc[transaction_index]
            break
        except:
            print("Not an integer value or out of range...")
    if transaction_index == -1:
        break
    if transaction_index == -2:
        ipdb.set_trace()
        print("Exiting without saving")
        break
    if transaction_index == -3:
        exit()
    print("\n      ***** Transaction Details ******         \n")
    print(df.loc[transaction_index])
    print(df.loc[transaction_index, "note"])
    print("\n      ***** All Categories ******         \n")
    for i in range(len(all_categories)):
        print(f"{i}: {all_categories[i]}")
    inputted_category = input(
        f"\nCategorize this transaction by typing in category or selecting index of pre-existing category (enter to skip):\n"
    )
    if inputted_category != "":
        if inputted_category.isdigit():
            inputted_category = all_categories[int(inputted_category)]
            df.loc[transaction_index, "category"] = inputted_category
        else:
            inputted_category = inputted_category.strip("/").lower()
            df.loc[transaction_index, "category"] = inputted_category
        while True:
            print("\n      ***** All Patterns ******         \n")
            for i in range(len(all_patterns)):
                print(f"{i}: {all_patterns[i]}")
            inputted_pattern = input(
                f"Add a pattern for this transaction. Assume text is lower-cased. (enter to skip)\n\n{df.loc[transaction_index, 'note']}\n"
            )
            if inputted_pattern == "":
                if inputted_category not in all_categories:
                    all_categories.append(inputted_category)
                break
            try:
                if inputted_pattern.isdigit():
                    inputted_pattern = all_patterns[int(inputted_pattern)]
                make_sure_pattern_matches_text(
                    inputted_pattern, df.loc[transaction_index, "note"]
                )
                if (
                    inputted_pattern,
                    inputted_category,
                ) not in pattern_category_map_list:
                    pattern_category_map_list_copy = deepcopy(pattern_category_map_list)
                    pattern_category_map_list_copy.append(
                        (inputted_pattern, inputted_category)
                    )
                    make_sure_no_pattern_maps_to_more_than_one_category(
                        pattern_category_map_list_copy
                    )
                    pattern_category_map_list.append(
                        (inputted_pattern, inputted_category)
                    )
                    pattern_category_map_dict[inputted_pattern] = inputted_category
                    df.loc[transaction_index, "pattern"] = inputted_pattern
                    if inputted_category not in all_categories:
                        all_categories.append(inputted_category)
                    if inputted_pattern not in all_patterns:
                        all_patterns.append(inputted_pattern)
                break
            except:
                print("Error: inputted pattern does not match text (note)")

print("Writing history.")
df.to_csv(data_fol_path + "history.csv", index=False)

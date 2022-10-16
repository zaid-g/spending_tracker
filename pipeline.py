from pprint import pprint as pp
import ipdb
import re
import json
from pprint import pprint as p
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


# ---------- [read csv file names and make sure no problems] ----------:


def contains_date_range(file_name):
    match_ = re.search(r"^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}", file_name)
    if match_ == None:
        return False
    return True


data_fol_path = sys.argv[1] + "/"
historical_categorized_csv_path = data_fol_path + "history.csv"
raw_csv_path = data_fol_path + "csv/raw/"
cleaned_csv_path = data_fol_path + "csv/cleaned/"
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


def rm_chars(s):
    chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    l = list(s)
    l = [c for c in l if c in chars]
    return float("".join(l))


def is_not_formatted(file_name):
    match_ = re.search(r"\d{4}-\d{2}_formatted____", file_name)
    if match_ == None:
        return True
    return False


def merge_debit_credit_columns(row):
    if np.isnan(row["Debit"]):
        return row["Credit"]
    else:
        return row["Debit"]


def venmo(file_path, file_name):
    df = pd.read_csv(
        file_path,
        skiprows=[0, 1],
    )
    df = df[df.columns[1:]]
    df = df.drop(0).reset_index()
    for i in range(len(df)):
        if pd.isna(df.ID[i]):
            break
    df = df.iloc[0:i]
    df["Note"] = df.apply(
        lambda row: row["Note"] + "__From: " + row["From"] + ". To: " + row["To"],
        axis=1,
    )
    df = df[["Datetime", "Amount (total)", "Note"]]
    df.columns = ["datetime", "amount", "note"]
    chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    df["amount"] = df["amount"].apply(lambda x: rm_chars(x))
    df["source"] = "venmo"
    df["preselected_category"] = None
    df = df[["datetime", "amount", "source", "preselected_category", "note"]]
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest(), axis=1
    )
    df.to_csv(cleaned_csv_path + "venmo_" + file_name, index=False)
    print("Cleaned one venmo file...")


def amex(file_path, file_name):
    df = pd.read_csv(
        file_path,
    )
    df = df[["Date", "Amount", "Description"]]
    df.columns = ["datetime", "amount", "note"]
    df["source"] = "amex"
    df["preselected_category"] = None
    df = df[["datetime", "amount", "source", "preselected_category", "note"]]
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest(), axis=1
    )
    df.to_csv(cleaned_csv_path + "amex" + file_name, index=False)
    print("Cleaned one amex file...")


def citi(file_path, file_name):
    df = pd.read_csv(
        file_path,
    )
    if (sum(np.isnan(df.Credit.values)) + sum(np.isnan(df.Debit.values))) != len(df):
        raise Exception("Failed to parse debit/credit columns")
    df["amount"] = df.apply(lambda row: merge_debit_credit_columns(row), axis=1)
    df = df[["Date", "amount", "Description"]]
    df.columns = ["datetime", "amount", "note"]
    df["source"] = "citi"
    df["preselected_category"] = None
    df = df[["datetime", "amount", "source", "preselected_category", "note"]]
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest(), axis=1
    )
    df.to_csv(cleaned_csv_path + "citi" + file_name, index=False)
    print("Cleaned one citi file...")


def amazon(file_path, file_name):
    df = pd.read_csv(
        file_path,
    )
    df = df[["Order Date", "Item Total", "Category", "Title"]]
    df.columns = ["datetime", "amount", "preselected_category", "note"]
    df["source"] = "amazon"
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["amount"] = df["amount"].apply(lambda x: rm_chars(x))
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest(), axis=1
    )
    df.to_csv(cleaned_csv_path + "amazon" + file_name, index=False)
    print("Cleaned one amazon file...")


def chase(file_path, file_name):
    pass


def detect_file_source(file_path):
    try:
        df = pd.read_csv(file_path, skiprows=[0, 1])
        if list(df.columns) == [
            "Unnamed: 0",
            "ID",
            "Datetime",
            "Type",
            "Status",
            "Note",
            "From",
            "To",
            "Amount (total)",
            "Amount (tip)",
            "Amount (fee)",
            "Funding Source",
            "Destination",
            "Beginning Balance",
            "Ending Balance",
            "Statement Period Venmo Fees",
            "Terminal Location",
            "Year to Date Venmo Fees",
            "Disclaimer",
        ]:
            return venmo
    except:
        pass
    try:
        df = pd.read_csv(
            file_path,
        )
        if list(df.columns) == [
            "Order Date",
            "Order ID",
            "Title",
            "Category",
            "ASIN/ISBN",
            "UNSPSC Code",
            "Website",
            "Release Date",
            "Condition",
            "Seller",
            "Seller Credentials",
            "List Price Per Unit",
            "Purchase Price Per Unit",
            "Quantity",
            "Payment Instrument Type",
            "Purchase Order Number",
            "PO Line Number",
            "Ordering Customer Email",
            "Shipment Date",
            "Shipping Address Name",
            "Shipping Address Street 1",
            "Shipping Address Street 2",
            "Shipping Address City",
            "Shipping Address State",
            "Shipping Address Zip",
            "Order Status",
            "Carrier Name & Tracking Number",
            "Item Subtotal",
            "Item Subtotal Tax",
            "Item Total",
            "Tax Exemption Applied",
            "Tax Exemption Type",
            "Exemption Opt-Out",
            "Buyer Name",
            "Currency",
            "Group Name",
        ]:
            return amazon
    except:
        pass
    try:
        df = pd.read_csv(
            file_path,
        )
        if list(df.columns) == ["Status", "Date", "Description", "Debit", "Credit"]:
            return citi
    except:
        pass
    try:
        df = pd.read_csv(
            file_path,
        )
        if list(df.columns) == [
            "Date",
            "Description",
            "Card Member",
            "Account #",
            "Amount",
        ]:
            return amex
    except:
        pass
    raise Exception(f"Could not identify file {file_path}")


for file_name in raw_csv_file_names:
    file_path = raw_csv_path + file_name
    detect_file_source(file_path)(file_path, file_name)

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

print(df[["datetime", "amount", "source", "note"]])

# first read entire df including written history
hist_df = pd.read_csv(historical_categorized_csv_path, parse_dates=["datetime"])

# assert all patterns in historical file indeed do match text of that transaction
def assert_pattern_matches_text(pattern, text):
    if pd.isna(pattern):
        return
    assert (
        re.compile(pattern).search(text) != None
    ), "Error: found pattern that doesn't match note (text)"


hist_df.apply(
    lambda row: assert_pattern_matches_text(row["pattern"], row["note"]), axis=1
)

# store all possible categories and patterns in variable
category_map_regex = list(
    set(
        [
            (row["pattern"], row["category"])
            for index, row in hist_df.iterrows()
            if not pd.isna(row["pattern"])
        ]
    )
)
# make sure no pattern maps to more than one category
all_patterns = set([pattern for pattern, _ in category_map_regex])
all_categories = set([category for _, category in category_map_regex])
for pattern in all_patterns:
    mapped_categories = set()
    for i in range(len(category_map_regex)):
        if category_map_regex[i][0] == pattern:
            mapped_categories.add(category_map_regex[i][1])
    assert (
        len(mapped_categories) == 1
    ), "Error: Found the same pattern mapping to more than one category"
category_map_regex = dict(category_map_regex)

# assert all categories are valid in historical file
def assert_category_dtype(category):
    if pd.isna(category):
        return
    assert type(category) == str, "category must be string"


hist_df["category"].apply(lambda category: assert_category_dtype(category))

# make sure no ids are duplicated
assert len(hist_df) == len(
    hist_df.id.value_counts()
), "Error: found duplicate IDs in historical file"

# make sure all historical transactions are accounted for in cleaned_csvs
assert set(hist_df.id.values).issubset(
    df.id.values
), "Error: not all historical transactions accounted for in cleaned or raw csvs"

# apply patterns on new data (df)
def get_matched_pattern(text):
    for pattern in category_map_regex:
        if re.compile(pattern).search(text) != None:
            return pattern


def get_category_from_pattern(pattern):
    if pattern == None:
        return None
    else:
        return category_map_regex[pattern]


df["pattern"] = df["note"].apply(lambda text: get_matched_pattern(text))
df["category"] = df["pattern"].apply(lambda pattern: get_category_from_pattern(pattern))


# update df to include history
df = df[~df.id.isin(hist_df.id.values)]
df = pd.concat([df, hist_df], axis=0, ignore_index=True)
df = df.sort_values("datetime", ascending=False, ignore_index=True)
# make sure no ids are duplicated
assert len(hist_df) == len(
    hist_df.id.value_counts()
), "Error: found duplicate IDs in concatenated history + recent file (really weird if you get this error)"

# time to ask user to confirm or override
# sort categories and patterns for visual display
all_categories = sorted(list(all_categories))
all_patterns = sorted(list(all_patterns))
while True:
    print(
        df[
            [
                "datetime",
                "amount",
                "note",
                "pattern",
                "category",
                "source",
                "preselected_category",
            ]
        ]
    )

    while True:
        try:
            transaction_index = int(
                input(
                    "\nSelect row you would like to categorize.\nEnter -1 if this looks good.\nEnter -2 for breakpoint.\nEnter -3 to quit\n"
                )
            )
            if transaction_index >= 0:
                df.loc[transaction_index]
            break
        except ValueError:
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
    print("\n      ***** All Categories ******         \n")
    for i in range(len(all_categories)):
        print(f"{i}: {all_categories[i]}")
    inputted_category = input(
        f"\nCategorize this transaction by typing in category or selecting index of pre-existing category:\n"
    )
    if inputted_category.isdigit():
        df.loc[transaction_index, "category"] = all_categories[int(inputted_category)]
    else:
        df.loc[transaction_index, "category"] = inputted_category
    while True:
        print("\n      ***** All Patterns ******         \n")
        for i in range(len(all_patterns)):
            print(f"{i}: {all_patterns[i]}")
        inputted_pattern = input("Add a pattern for this transaction (enter to skip)\n")
        if inputted_pattern == "":
            break
        try:
            if inputted_pattern.isdigit():
                inputted_pattern = all_patterns[int(inputted_pattern)]
            assert_pattern_matches_text(
                inputted_pattern, df.loc[transaction_index, "note"]
            )
            df.loc[transaction_index, "pattern"] = inputted_pattern
            break
        except:
            print("Error: inputted pattern does not match text (note)")

print("Writing to history.csv file")
df.to_csv(data_fol_path + "history.csv", index=False)

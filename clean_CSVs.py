import re
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


data_fol_path = sys.argv[1]
raw_csv_path = data_fol_path + "/csv/raw/"
cleaned_csv_path = data_fol_path + "/csv/cleaned/"
csv_file_names = [f for f in listdir(raw_csv_path) if isfile(join(raw_csv_path, f))]
csv_file_names = [
    file_name for file_name in csv_file_names if file_name[0] != "."
]  # remove hidden csv_file_names


# make sure all csv_file_names have date range
for file_name in csv_file_names:
    if not contains_date_range(file_name):
        raise Exception(
            f"No date range detected in file {file}. Format is \n^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}"
        )


# ---------- [rm whitespace, lowercase csv file names, reread names] ----------:

for file_name in csv_file_names:
    file_path = raw_csv_path + file_name
    formatted_file_name = "".join(file_name.split()).lower()
    formatted_file_path = raw_csv_path + formatted_file_name
    os.rename(file_path, formatted_file_path)

csv_file_names = [f for f in listdir(raw_csv_path) if isfile(join(raw_csv_path, f))]
csv_file_names = [
    file_name for file_name in csv_file_names if file_name[0] != "."
]  # remove hidden csv_file_names


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
        lambda row: row["Note"] + "__From: " + row["From"] + ". To: " + row["To"], axis=1
    )
    df = df[["Datetime", "Amount (total)", "Note"]]
    df.columns = ["datetime", "amount", "note"]
    chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    df["amount"] = df["amount"].apply(lambda x: rm_chars(x))
    df["source"] = "venmo"
    df["preselected_category"] = None
    df = df[["datetime", "amount", "source", "preselected_category", "note"]]
    df.to_csv(cleaned_csv_path + "venmo_" + file_name)
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
    df.to_csv(cleaned_csv_path + "amex" + file_name)
    print("Cleaned one amex file...")


def citi(file_path, file_name):
    df = pd.read_csv(
        file_path,
    )
    if (sum(np.isnan(df.Credit.values)) + sum(np.isnan(df.Debit.values))) != len(df):
        raise Exception("Failed to parse debit/credit columns")
    df["amount"] = df.apply(lambda row: merge_debit_credit_columns(row), axis=1)
    df = df[[ "Date", "amount", "Description"]]
    df.columns = [ "datetime", "amount", "note"]
    df["source"] = "citi"
    df["preselected_category"] = None
    df = df[[ "datetime", "amount", "source", "preselected_category", "note"]]
    df.to_csv(cleaned_csv_path + "citi" + file_name)
    print("Cleaned one citi file...")


def amazon(file_path, file_name):
    df = pd.read_csv(
        file_path,
    )
    df = df[["Order Date", "Item Total", "Category", "Title"]]
    df.columns = ["datetime", "amount", "preselected_category", "note"]
    df["amount"] = df["amount"].apply(lambda x: rm_chars(x))
    df.to_csv(cleaned_csv_path + "amazon" + file_name)
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


for file_name in csv_file_names:
    file_path = raw_csv_path + file_name
    detect_file_source(file_path)(file_path, file_name)

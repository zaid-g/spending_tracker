import re
import sys
import numpy as np
import pandas as pd
import os
from os import listdir
from os.path import isfile, join

data_fol_path = sys.argv[1]
raw_csv_path = data_fol_path + "/csv/raw"
cleaned_csv_path = data_fol_path + "/csv/cleaned/"
csv_files = [f for f in listdir(raw_csv_path) if isfile(join(raw_csv_path, f))]

csv_files = [file for file in csv_files if file[0] != "."]  # remove hidden csv_files


def contains_date_range(file):
    match_ = re.search(r"^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}", file)
    if match_ == None:
        return False
    return True


# make sure all csv_files have date range
for file in csv_files:
    if not contains_date_range(file):
        raise Exception(
            f"No date range detected in file {file}. Format is \n^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}"
        )


def is_not_formatted(file):
    match_ = re.search(r"\d{4}-\d{2}_formatted____", file)
    if match_ == None:
        return True
    return False


unformatted_files = [file for file in csv_files if is_not_formatted(file)]
print(f"Unformatted files are {unformatted_files}")


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
            return "venmo"
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
            return "amazon"
    except:
        pass
    try:
        df = pd.read_csv(
            file_path,
        )
        if list(df.columns) == ["Status", "Date", "Description", "Debit", "Credit"]:
            return "citi"
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
            return "amex"
    except:
        pass
    raise Exception(f"Could not identify file {file_path}")


for file in csv_files:
    file_path = raw_csv_path + "/" + file
    os.rename(file_path, ''.join(file_path.split()))
    file_path = ''.join(file_path.split())
    source_name = detect_file_source(file_path)
    cpy_command = f"cp {file_path} {cleaned_csv_path + source_name + file}"
    os.system(cpy_command)

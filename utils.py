import dateutil
import pandas as pd
import hashlib
import numpy as np
import re


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


def venmo(file_path, file_name, cleaned_csv_path):
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


def amex(file_path, file_name, cleaned_csv_path):
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


def citi(file_path, file_name, cleaned_csv_path):
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


def amazon(file_path, file_name, cleaned_csv_path):
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


def chase(file_path, file_name, cleaned_csv_path):
    df = pd.read_csv(
        file_path,
        index_col=False
    )
    df = df[["Posting Date", "Amount", "Description"]]
    df.columns = ["datetime", "amount", "note"]
    df["source"] = "chase"
    df["preselected_category"] = None
    df = df[["datetime", "amount", "source", "preselected_category", "note"]]
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest(), axis=1
    )
    df.to_csv(cleaned_csv_path + "chase" + file_name, index=False)
    print("Cleaned one chase file...")


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
    try:
        df = pd.read_csv(
            file_path,
        )
        if list(df.columns) == ['Details', 'Posting Date', 'Description', 'Amount', 'Type', 'Balance', 'Check or Slip #']:
            return chase
    except:
        pass
    raise Exception(f"Could not identify file {file_path}")


def contains_date_range(file_name):
    match_ = re.search(r"^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}", file_name)
    if match_ == None:
        return False
    return True


def get_all_patterns_categories(category_map_regex):
    all_patterns = sorted(list(set([pattern for pattern, _ in category_map_regex])))
    all_categories = sorted(list(set([category for _, category in category_map_regex])))
    return all_patterns, all_categories


def make_sure_no_pattern_maps_to_more_than_one_category(category_map_regex):
    all_patterns, all_categories = get_all_patterns_categories(category_map_regex)
    for pattern in all_patterns:
        mapped_categories = set()
        for i in range(len(category_map_regex)):
            if category_map_regex[i][0] == pattern:
                mapped_categories.add(category_map_regex[i][1])
        assert (
            len(mapped_categories) == 1
        ), "Error: Found the same pattern mapping to more than one category"


def make_sure_category_dtype(category):
    if pd.isna(category):
        return
    assert type(category) == str, "category must be string"


def make_sure_pattern_matches_text(pattern, text):
    text = text.lower()
    if pd.isna(pattern):
        return
    assert (
        re.compile(pattern).search(text) != None
    ), "Error: found pattern that doesn't match note (text)"


def extract_patterns_categories_from_history(hist_df):
    """returns tuple and dict objects"""
    category_map_regex = list(
        set(
            [
                (row["pattern"], row["category"])
                for index, row in hist_df.iterrows()
                if not pd.isna(row["pattern"])
            ]
        )
    )
    category_map_regex_dict = dict(category_map_regex)
    return category_map_regex, category_map_regex_dict


def get_matched_pattern(text, category_map_regex_dict):
    for pattern in category_map_regex_dict:
        if re.compile(pattern).search(text) != None:
            return pattern


def get_category_from_pattern(pattern, category_map_regex_dict):
    if pattern == None:
        return None
    else:
        return category_map_regex_dict[pattern]

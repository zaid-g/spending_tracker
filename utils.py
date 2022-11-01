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
    df["amount"] = df["amount"].apply(lambda x: -rm_chars(x))
    df["source"] = "venmo"
    df["preselected_category"] = None
    df = df[["datetime", "amount", "source", "preselected_category", "note"]]
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["note"] = df["source"] + "_" + df["note"]
    df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[0:30],
        axis=1,
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
    df["note"] = df["source"] + "_" + df["note"]
    df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[0:30],
        axis=1,
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
    df["note"] = df["source"] + "_" + df["note"]
    df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[0:30],
        axis=1,
    )
    df.to_csv(cleaned_csv_path + "citi" + file_name, index=False)
    print("Cleaned one citi file...")


def amazon_refunds(file_path, file_name, cleaned_csv_path):
    df = pd.read_csv(
        file_path,
    )
    df["amount"] = df["Refund Amount"].apply(lambda x: rm_chars(x)) + df[
        "Refund Tax Amount"
    ].apply(lambda x: rm_chars(x))
    df["amount"] = df["amount"] * -1
    df = df[["Order Date", "amount", "Category", "Title"]]
    df.columns = ["datetime", "amount", "preselected_category", "note"]
    df["source"] = "amazon_refunds"
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["note"] = df["source"] + "_" + df["note"]
    df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[0:30],
        axis=1,
    )
    df.to_csv(cleaned_csv_path + "amazon_refunds" + file_name, index=False)
    print("Cleaned one amazon_refunds file...")


def amazon_items(file_path, file_name, cleaned_csv_path):
    df = pd.read_csv(
        file_path,
    )
    df = df[["Order Date", "Item Total", "Category", "Title"]]
    df.columns = ["datetime", "amount", "preselected_category", "note"]
    df["source"] = "amazon_items"
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["amount"] = df["amount"].apply(lambda x: rm_chars(x))
    df["note"] = df["source"] + "_" + df["note"]
    df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[0:30],
        axis=1,
    )
    df.to_csv(cleaned_csv_path + "amazon_items" + file_name, index=False)
    print("Cleaned one amazon_items file...")


def chase(file_path, file_name, cleaned_csv_path):
    df = pd.read_csv(file_path, index_col=False)
    df = df[["Posting Date", "Amount", "Description"]]
    df.columns = ["datetime", "amount", "note"]
    df["amount"] = df["amount"].apply(lambda x: -x)
    df["source"] = "chase"
    df["preselected_category"] = None
    df = df[["datetime", "amount", "source", "preselected_category", "note"]]
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["note"] = df["source"] + "_" + df["note"]
    df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[0:30],
        axis=1,
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
            return amazon_items
    except:
        pass
    try:
        df = pd.read_csv(
            file_path,
        )
        if list(df.columns) == [
            "Order ID",
            "Order Date",
            "Title",
            "Category",
            "ASIN/ISBN",
            "Website",
            "Purchase Order Number",
            "Refund Date",
            "Refund Condition",
            "Refund Amount",
            "Refund Tax Amount",
            "Tax Exemption Applied",
            "Refund Reason",
            "Quantity",
            "Seller",
            "Seller Credentials",
            "Buyer Name",
            "Group Name",
        ]:
            return amazon_refunds
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
        if list(df.columns) == [
            "Details",
            "Posting Date",
            "Description",
            "Amount",
            "Type",
            "Balance",
            "Check or Slip #",
        ]:
            return chase
    except:
        pass
    raise Exception(f"Could not identify file {file_path}")


def contains_date_range(file_name):
    match_ = re.search(r"^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}", file_name)
    if match_ == None:
        return False
    return True


def make_sure_no_pattern_maps_to_more_than_one_category(pattern_category_map_list):
    patterns = sorted(list(set([pattern for pattern, _ in pattern_category_map_list])))
    categories = sorted(
        list(set([category for _, category in pattern_category_map_list]))
    )
    for pattern in patterns:
        mapped_categories = set()
        for i in range(len(pattern_category_map_list)):
            if pattern_category_map_list[i][0] == pattern:
                mapped_categories.add(pattern_category_map_list[i][1])
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
    ), f"Error: found pattern that doesn't match note (text). Pattern: {pattern} --- Text: {text}"


def extract_patterns_categories_from_history(hist_df):
    """returns tuple and dict objects"""
    pattern_category_map_list = list(
        set(
            [
                (row["pattern"], row["category"])
                for index, row in hist_df.iterrows()
                if not pd.isna(row["pattern"])
            ]
        )
    )
    pattern_category_map_dict = dict(pattern_category_map_list)
    all_categories = [i for i in hist_df["category"].unique() if not pd.isna(i)]
    all_patterns = [i for i in hist_df["pattern"].unique() if not pd.isna(i)]
    return (
        pattern_category_map_list,
        pattern_category_map_dict,
        all_categories,
        all_patterns,
    )


def get_matched_pattern(text, pattern_category_map_dict):
    for pattern in pattern_category_map_dict:
        if re.compile(pattern).search(text) != None:
            return pattern


def get_category_from_pattern(pattern, pattern_category_map_dict):
    if pattern == None:
        return None
    else:
        return pattern_category_map_dict[pattern]

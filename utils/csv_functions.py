import dateutil
import pandas as pd
import hashlib
import numpy as np


def rm_chars(s):
    chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    l = list(s)
    l = [c for c in l if c in chars]
    return float("".join(l))


def merge_debit_credit_columns(row):
    if np.isnan(row["Debit"]):
        return row["Credit"]
    else:
        return row["Debit"]


def venmo(file_path, file_name, cleaned_csv_path):
    source = "venmo"
    assert (
        source in file_name
    ), f"Error: source '{source}' is not in file name '{file_name}'"
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
    df["source"] = source
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
    df.to_csv(cleaned_csv_path + file_name, index=False)
    print("Cleaned one venmo file...")


def amex(file_path, file_name, cleaned_csv_path):
    source = "amex"
    assert (
        source in file_name
    ), f"Error: source '{source}' is not in file name '{file_name}'"
    df = pd.read_csv(
        file_path,
    )
    df = df[["Date", "Amount", "Description"]]
    df.columns = ["datetime", "amount", "note"]
    df["source"] = source
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
    df.to_csv(cleaned_csv_path + file_name, index=False)
    print("Cleaned one amex file...")


def citi(file_path, file_name, cleaned_csv_path):
    source = "citi"
    assert (
        source in file_name
    ), f"Error: source '{source}' is not in file name '{file_name}'"
    df = pd.read_csv(
        file_path,
    )
    if (sum(np.isnan(df.Credit.values)) + sum(np.isnan(df.Debit.values))) != len(df):
        raise Exception("Failed to parse debit/credit columns")
    df["amount"] = df.apply(lambda row: merge_debit_credit_columns(row), axis=1)
    df = df[["Date", "amount", "Description"]]
    df.columns = ["datetime", "amount", "note"]
    df["source"] = source
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
    df.to_csv(cleaned_csv_path + file_name, index=False)
    print("Cleaned one citi file...")


def amazon_refunds(file_path, file_name, cleaned_csv_path):
    source = "amazon_refunds"
    assert (
        source in file_name
    ), f"Error: source '{source}' is not in file name '{file_name}'"
    df = pd.read_csv(
        file_path,
    )
    df["amount"] = df["Refund Amount"].apply(lambda x: rm_chars(x)) + df[
        "Refund Tax Amount"
    ].apply(lambda x: rm_chars(x))
    df["amount"] = df["amount"] * -1
    df = df[["Order Date", "amount", "Category", "Title"]]
    df.columns = ["datetime", "amount", "preselected_category", "note"]
    df["source"] = source
    df["datetime"] = df["datetime"].apply(
        lambda datetime_string: dateutil.parser.parse(datetime_string)
    )
    df["note"] = df["source"] + "_" + df["note"]
    df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
    df["id"] = df.apply(
        lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[0:30],
        axis=1,
    )
    df.to_csv(cleaned_csv_path + file_name, index=False)
    print("Cleaned one amazon_refunds file...")


def amazon_items(file_path, file_name, cleaned_csv_path):
    source = "amazon_items"
    assert (
        source in file_name
    ), f"Error: source '{source}' is not in file name '{file_name}'"
    df = pd.read_csv(
        file_path,
    )
    df = df[["Order Date", "Item Total", "Category", "Title"]]
    df.columns = ["datetime", "amount", "preselected_category", "note"]
    df["source"] = source
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
    df.to_csv(cleaned_csv_path + file_name, index=False)
    print("Cleaned one amazon_items file...")


def chase_freedom(file_path, file_name, cleaned_csv_path):
    source = "chase_freedom"
    assert (
        source in file_name
    ), f"Error: source '{source}' is not in file name '{file_name}'"
    df = pd.read_csv(file_path, index_col=False)
    # Make sure to get Post date not transaction date, that's what website
    # search tool uses to filter/search
    df = df[["Post Date", "Description", "Category", "Amount"]]
    df.columns = ["datetime", "note", "preselected_category", "amount"]
    df["amount"] = df["amount"].apply(lambda x: -x)
    df["source"] = source
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
    df.to_csv(cleaned_csv_path + file_name, index=False)
    print("Cleaned one chase freedom file...")


def chase_debit(file_path, file_name, cleaned_csv_path):
    source = "chase_debit"
    assert (
        source in file_name
    ), f"Error: source '{source}' is not in file name '{file_name}'"
    df = pd.read_csv(file_path, index_col=False)
    df = df[["Posting Date", "Amount", "Description"]]
    df.columns = ["datetime", "amount", "note"]
    df["amount"] = df["amount"].apply(lambda x: -x)
    df["source"] = source
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
    df.to_csv(cleaned_csv_path + file_name, index=False)
    print("Cleaned one chase debit file...")


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
            return chase_debit
    except:
        pass
    try:
        df = pd.read_csv(
            file_path,
        )
        if list(df.columns) == [
            "Transaction Date",
            "Post Date",
            "Description",
            "Category",
            "Type",
            "Amount",
            "Memo",
        ]:
            return chase_freedom
    except:
        pass
    raise Exception(f"Could not identify file {file_path}")

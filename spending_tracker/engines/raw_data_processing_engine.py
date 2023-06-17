import re
import json
import datetime
from dateutil import parser
import sys
import os
import os
import dateutil
import pandas as pd
import hashlib
import numpy as np


class RawDataProcessingEngine:
    def __init__(self, root_data_folder_path: str):
        self.root_data_folder_path = root_data_folder_path
        self.raw_data_folder_path = self.root_data_folder_path + "raw/"
        self.raw_data_file_names = self.read_raw_data_file_names()
        self.cleaned_data_folder_path = self.root_data_folder_path + "cleaned/"
        self.historical_transactions_file_path = (
            self.root_data_folder_path + "history.csv"
        )
        with open(self.root_data_folder_path + "config.json") as f:
            self.config = json.load(f)
        self.validate_data_folder_structure()

    def read_raw_data_file_names(self):
        raw_data_file_names = [
            f
            for f in os.listdir(self.raw_data_folder_path)
            if (
                os.path.isfile(os.path.join(self.raw_data_folder_path, f))
                and (f[0] != ".")
            )  # TODO test
        ]
        return raw_data_file_names

    def validate_data_folder_structure(self):
        # TODO
        pass

    @staticmethod
    def contains_date_range(file_name):
        match_ = re.search(r"^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}", file_name)
        if match_ == None:
            return False
        return True

    def verify_raw_data_file_names_contain_date_and_valid_account_name(self):
        accounts = self.config["accounts"]
        for raw_data_file_name in raw_data_file_names:
            found_account = False
            for account in accounts:
                if account in raw_data_file_name:
                    found_account = True
            if not found_account:
                # make sure all raw_data_file_names have a valid account name
                raise ValueError(
                    f"File {raw_data_file_name} does not match any existing account name {accounts}"
                )
            if not self.contains_date_range(raw_data_file_name):
                # make sure all raw_data_file_names have date range
                raise ValueError(
                    f"No date range detected in file {file}. Format is \n^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}"
                )

    @staticmethod
    def verify_raw_data_file_name_contains_proper_date_ranges_for_each_account(
        account_raw_data_file_names
    ):
        # first verify that to date > from date
        for i in range(len(account_raw_data_file_names)):
            from_date = parser.parse(
                account_raw_data_file_names[i][0:4]
                + account_raw_data_file_names[i][5:7]
                + account_raw_data_file_names[i][8:10]
            )
            to_date = parser.parse(
                account_raw_data_file_names[i][14:18]
                + account_raw_data_file_names[i][19:21]
                + account_raw_data_file_names[i][22:24]
            )
            if to_date <= from_date:
                raise ValueError(
                    f"Error: Found file with from date greater than to date {account_raw_data_file_names[i]}"
                )
        # next verify no gaps/overlaps in date ranges
        for i in range(len(account_raw_data_file_names) - 1):
            from_date_next = parser.parse(
                account_raw_data_file_names[i + 1][0:4]
                + account_raw_data_file_names[i + 1][5:7]
                + account_raw_data_file_names[i + 1][8:10]
            )
            to_date = parser.parse(
                account_raw_data_file_names[i][14:18]
                + account_raw_data_file_names[i][19:21]
                + account_raw_data_file_names[i][22:24]
            )
            if not (from_date_next - to_date == datetime.timedelta(days=1)):
                raise ValueError(
                    f"Error: Found gaps/overlaps in date range for  {account_raw_data_file_names[i], account_raw_data_file_names[i+1]}"
                )

    def verify_raw_data_file_names_contain_proper_date_ranges_for_each_account(self):
        for account in self.config["accounts"]:
            account_raw_data_file_names = [
                f for f in self.raw_data_file_names if account in f
            ]
            account_raw_data_file_names.sort(
                key=lambda f: int(f[0:4] + f[5:7] + f[8:10])
            )
            self.verify_raw_data_file_name_contains_proper_date_ranges_for_each_account(
                account_raw_data_file_names
            )

    def format_raw_data_file_names(self):
        for raw_data_file_name in self.raw_data_file_names:
            raw_data_file_path = self.raw_data_folder_path + raw_data_file_name
            formatted_raw_data_file_name = (
                "".join(raw_data_file_name.split())
                .lower()
                .replace("(", "")
                .replace(")", "")
            )
            formatted_raw_data_file_path = (
                raw_data_file_path + formatted_raw_data_file_name
            )
            os.rename(raw_data_file_path, formatted_raw_data_file_path)
        self.raw_data_file_names = self.read_raw_data_file_names()

    @staticmethod
    def remove_non_numerical_chars(s):
        chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        l = list(s)
        l = [c for c in l if c in chars]
        return float("".join(l))

    def clean_raw_data_files(self):
        for raw_data_file_name in self.raw_data_file_names:
            raw_data_file_path = self.raw_data_folder_path + raw_data_file_name
            self.detect_file_source(raw_data_file_path)(
                raw_data_file_path, raw_data_file_name
            )

    def merge_debit_credit_columns(self, row):
        if np.isnan(row["Debit"]):
            return row["Credit"]
        else:
            return row["Debit"]

    def venmo(self, file_path, file_name):
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
        df["amount"] = df["amount"].apply(lambda x: -remove_non_numerical_chars(x))
        df["source"] = source
        df["third_party_category"] = None
        df = df[["datetime", "amount", "source", "third_party_category", "note"]]
        df["datetime"] = df["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        df["note"] = df["source"] + "_" + df["note"]
        df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
        df["id"] = df.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        df.to_csv(self.cleaned_data_folder_path + file_name, index=False)
        print("Cleaned one venmo file...")

    def amex(self, file_path, file_name):
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
        df["third_party_category"] = None
        df = df[["datetime", "amount", "source", "third_party_category", "note"]]
        df["datetime"] = df["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        df["note"] = df["source"] + "_" + df["note"]
        df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
        df["id"] = df.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        df.to_csv(self.cleaned_data_folder_path + file_name, index=False)
        print("Cleaned one amex file...")

    def citi(self, file_path, file_name):
        source = "citi"
        assert (
            source in file_name
        ), f"Error: source '{source}' is not in file name '{file_name}'"
        df = pd.read_csv(
            file_path,
        )
        if (sum(np.isnan(df.Credit.values)) + sum(np.isnan(df.Debit.values))) != len(
            df
        ):
            raise Exception("Failed to parse debit/credit columns")
        df["amount"] = df.apply(lambda row: merge_debit_credit_columns(row), axis=1)
        df = df[["Date", "amount", "Description"]]
        df.columns = ["datetime", "amount", "note"]
        df["source"] = source
        df["third_party_category"] = None
        df = df[["datetime", "amount", "source", "third_party_category", "note"]]
        df["datetime"] = df["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        df["note"] = df["source"] + "_" + df["note"]
        df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
        df["id"] = df.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        df.to_csv(self.cleaned_data_folder_path + file_name, index=False)
        print("Cleaned one citi file...")

    def amazon_refunds(self, file_path, file_name):
        source = "amazon_refunds"
        assert (
            source in file_name
        ), f"Error: source '{source}' is not in file name '{file_name}'"
        df = pd.read_csv(
            file_path,
        )
        df["amount"] = df["Refund Amount"].apply(
            lambda x: remove_non_numerical_chars(x)
        ) + df["Refund Tax Amount"].apply(lambda x: remove_non_numerical_chars(x))
        df["amount"] = df["amount"] * -1
        df = df[["Order Date", "amount", "Category", "Title"]]
        df.columns = ["datetime", "amount", "third_party_category", "note"]
        df["source"] = source
        df["datetime"] = df["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        df["note"] = df["source"] + "_" + df["note"]
        df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
        df["id"] = df.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        df.to_csv(self.cleaned_data_folder_path + file_name, index=False)
        print("Cleaned one amazon_refunds file...")

    def amazon_items(self, file_path, file_name):
        source = "amazon_items"
        assert (
            source in file_name
        ), f"Error: source '{source}' is not in file name '{file_name}'"
        df = pd.read_csv(
            file_path,
        )
        df = df[["Order Date", "Item Total", "Category", "Title"]]
        df.columns = ["datetime", "amount", "third_party_category", "note"]
        df["source"] = source
        df["datetime"] = df["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        df["amount"] = df["amount"].apply(lambda x: remove_non_numerical_chars(x))
        df["note"] = df["source"] + "_" + df["note"]
        df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
        df["id"] = df.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        df.to_csv(self.cleaned_data_folder_path + file_name, index=False)
        print("Cleaned one amazon_items file...")

    def chase_freedom(self, file_path, file_name):
        source = "chase_freedom"
        assert (
            source in file_name
        ), f"Error: source '{source}' is not in file name '{file_name}'"
        df = pd.read_csv(file_path, index_col=False)
        # Make sure to get Post date not transaction date, that's what website
        # search tool uses to filter/search
        df = df[["Post Date", "Description", "Category", "Amount"]]
        df.columns = ["datetime", "note", "third_party_category", "amount"]
        df["amount"] = df["amount"].apply(lambda x: -x)
        df["source"] = source
        df = df[["datetime", "amount", "source", "third_party_category", "note"]]
        df["datetime"] = df["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        df["note"] = df["source"] + "_" + df["note"]
        df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
        df["id"] = df.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        df.to_csv(self.cleaned_data_folder_path + file_name, index=False)
        print("Cleaned one chase freedom file...")

    def chase_debit(self, file_path, file_name):
        source = "chase_debit"
        assert (
            source in file_name
        ), f"Error: source '{source}' is not in file name '{file_name}'"
        df = pd.read_csv(file_path, index_col=False)
        df = df[["Posting Date", "Amount", "Description"]]
        df.columns = ["datetime", "amount", "note"]
        df["amount"] = df["amount"].apply(lambda x: -x)
        df["source"] = source
        df["third_party_category"] = None
        df = df[["datetime", "amount", "source", "third_party_category", "note"]]
        df["datetime"] = df["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        df["note"] = df["source"] + "_" + df["note"]
        df["note"] = df["note"].apply(lambda s: s.replace(",", "."))
        df["id"] = df.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        df.to_csv(self.cleaned_data_folder_path + file_name, index=False)
        print("Cleaned one chase debit file...")

    def detect_file_source(self, file_path):
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

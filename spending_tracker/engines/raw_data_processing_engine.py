import re
from dependency_injector import providers
from spending_tracker.engines.data_validation_engine import DataValidationEngine
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

    """This engine is for
    1) Validating raw (downloaded) data integrity.
    2) Processing raw files from various supported accounts (e.g. Chase, Amazon) into
    files with a cleaned and unified format.
    """

    def __init__(
        self,
        data_validation_engine: DataValidationEngine,
        root_data_folder_path: str,
    ):
        self.root_data_folder_path = root_data_folder_path
        self.raw_data_folder_path = self.root_data_folder_path + "raw/"
        self.processed_data_folder_path = self.root_data_folder_path + "processed/"

    def read_raw_data_file_names(self) -> list:
        raw_data_file_names = []
        for file_name in os.listdir(self.raw_data_folder_path):
            if os.path.isfile(os.path.join(self.raw_data_folder_path, file_name)) and (
                file_name[0] != "."
            ):
                raw_data_file_names.append(file_name)
        return raw_data_file_names

    def process_raw_data_files(self) -> None:
        self.data_validation_engine.verify_raw_data_file_names_contain_date_range(
            self.raw_data_file_names
        )
        self.data_validation_engine.verify_raw_data_file_names_contain_only_single_account(
            self.raw_data_file_names
        )
        self.data_validation_engine.verify_raw_data_file_names_contain_proper_date_ranges_for_each_account(
            self.raw_data_file_names
        )
        for raw_data_file_name in self.read_raw_data_file_names():
            raw_data_file_path = self.raw_data_folder_path + raw_data_file_name
            raw_data = pd.read_csv(raw_data_file_path)
            self.detect_file_source(raw_data_file_path)(
                raw_data_file_path, raw_data_file_name
            )

    @staticmethod
    def remove_non_numerical_chars(s) -> float:
        """This is to convert e.g. '$14.83' to '14,83'"""
        chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        l = list(s)
        l = [c for c in l if c in chars]
        return float("".join(l))

    def american_express_blue_cash_preferred(self, file_path, file_name):
        source = "american_express_bluecash_preferred"
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
        df.to_csv(self.processed_data_folder_path + file_name, index=False)
        print("Cleaned one american_express_bluecash_preferred file...")

    def citi_double_cash_2022(self, file_path, file_name):
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

        def merge_debit_credit_columns(self, row):
            if np.isnan(row["Debit"]):
                return row["Credit"]
            else:
                return row["Debit"]

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
        df.to_csv(self.processed_data_folder_path + file_name, index=False)
        print("Cleaned one citi file...")

    def citi_custom_cash_2022(self, file_path, file_name):
        return self.citi_double_cash_2022(file_path, file_name)

    def amazon_refunds_2022(self, file_path, file_name):
        source = "amazon_refunds_2022"
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
        df.to_csv(self.processed_data_folder_path + file_name, index=False)
        print("Cleaned one amazon_refunds_2022 file...")

    def amazon_items_2022(self, file_path, file_name):
        source = "amazon_items_2022"
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
        df.to_csv(self.processed_data_folder_path + file_name, index=False)
        print("Cleaned one amazon_items_2022 file...")

    def chase_freedom_unlimited_2022(self, file_path, file_name):
        source = "chase_freedom_unlimited"
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
        df.to_csv(self.processed_data_folder_path + file_name, index=False)
        print("Cleaned one chase freedom file...")

    def chase_debit_2022(self, file_path, file_name):
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
        df.to_csv(self.processed_data_folder_path + file_name, index=False)
        print("Cleaned one chase debit file...")

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
            account = self.detect_account_in_raw_data_file_name(raw_data_file_name)
            raw_data_file_path = self.raw_data_folder_path + raw_data_file_name
            raw_data = pd.read_csv(raw_data_file_path)
            raw_data["account"] = account
            # call function with the name of the account
            processed_data = globals()[account](
                raw_data, raw_data_file_path, raw_data_file_name
            )
            processed_data.to_csv(
                self.processed_data_folder_path + raw_data_file_name, index=False
            )

    def detect_account_in_raw_data_file_name(self, raw_data_file_name: str) -> str:
        for account in self.supported_accounts:
            if account in raw_data_file_name:
                return account

    @staticmethod
    def remove_non_numerical_chars(s) -> float:
        """This is to convert e.g. '$14.83' to '14.83'"""
        chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        l = list(s)
        l = [c for c in l if c in chars]
        return float("".join(l))

    def american_express_blue_cash_preferred_2022_1(
        self, raw_data, raw_data_file_path, raw_data_file_name
    ) -> pd.DataFrame:
        raw_data = raw_data[["Date", "Amount", "Description"]]
        raw_data.columns = ["datetime", "amount", "note"]
        raw_data["third_party_category"] = None
        raw_data = raw_data[
            ["datetime", "amount", "source", "third_party_category", "note"]
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["source"] + "_" + raw_data["note"]
        raw_data["note"] = raw_data["note"].apply(lambda s: s.replace(",", "."))
        raw_data["id"] = raw_data.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        return raw_data

    def citi_double_cash_2022_1(
        self, raw_data, raw_data_file_path, raw_data_file_name
    ) -> pd.DataFrame:
        if (
            sum(np.isnan(raw_data.Credit.values)) + sum(np.isnan(raw_data.Debit.values))
        ) != len(raw_data):
            raise Exception("Failed to parse debit/credit columns")

        def merge_debit_credit_columns(self, row):
            if np.isnan(row["Debit"]):
                return row["Credit"]
            else:
                return row["Debit"]

        raw_data["amount"] = raw_data.apply(
            lambda row: merge_debit_credit_columns(row), axis=1
        )
        raw_data = raw_data[["Date", "amount", "Description"]]
        raw_data.columns = ["datetime", "amount", "note"]
        raw_data["third_party_category"] = None
        raw_data = raw_data[
            ["datetime", "amount", "source", "third_party_category", "note"]
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["source"] + "_" + raw_data["note"]
        raw_data["note"] = raw_data["note"].apply(lambda s: s.replace(",", "."))
        raw_data["id"] = raw_data.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        return raw_data

    def citi_custom_cash_2022_1(
        self, raw_data, raw_data_file_path, raw_data_file_name
    ) -> pd.DataFrame:
        return self.citi_double_cash_2022(raw_data_file_path, raw_data_file_name)

    def amazon_refunds_2022_1(
        self, raw_data, raw_data_file_path, raw_data_file_name
    ) -> pd.DataFrame:
        raw_data["amount"] = raw_data["Refund Amount"].apply(
            lambda x: remove_non_numerical_chars(x)
        ) + raw_data["Refund Tax Amount"].apply(lambda x: remove_non_numerical_chars(x))
        raw_data["amount"] = raw_data["amount"] * -1
        raw_data = raw_data[["Order Date", "amount", "Category", "Title"]]
        raw_data.columns = ["datetime", "amount", "third_party_category", "note"]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["source"] + "_" + raw_data["note"]
        raw_data["note"] = raw_data["note"].apply(lambda s: s.replace(",", "."))
        raw_data["id"] = raw_data.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        return raw_data

    def amazon_items_2022_1(
        self, raw_data, raw_data_file_path, raw_data_file_name
    ) -> pd.DataFrame:
        raw_data = raw_data[["Order Date", "Item Total", "Category", "Title"]]
        raw_data.columns = ["datetime", "amount", "third_party_category", "note"]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["amount"] = raw_data["amount"].apply(
            lambda x: remove_non_numerical_chars(x)
        )
        raw_data["note"] = raw_data["source"] + "_" + raw_data["note"]
        raw_data["note"] = raw_data["note"].apply(lambda s: s.replace(",", "."))
        raw_data["id"] = raw_data.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        return raw_data

    def chase_freedom_unlimited_2022_1(
        self, raw_data, raw_data_file_path, raw_data_file_name
    ) -> pd.DataFrame:
        # Make sure to get Post date not transaction date, that's what website
        # search tool uses to filter/search
        raw_data = raw_data[["Post Date", "Description", "Category", "Amount"]]
        raw_data.columns = ["datetime", "note", "third_party_category", "amount"]
        raw_data["amount"] = raw_data["amount"].apply(lambda x: -x)
        raw_data = raw_data[
            ["datetime", "amount", "source", "third_party_category", "note"]
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["source"] + "_" + raw_data["note"]
        raw_data["note"] = raw_data["note"].apply(lambda s: s.replace(",", "."))
        raw_data["id"] = raw_data.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        return raw_data

    def chase_debit_2022_1(
        self, raw_data, raw_data_file_path, raw_data_file_name
    ) -> pd.DataFrame:
        raw_data = raw_data[["Posting Date", "Amount", "Description"]]
        raw_data.columns = ["datetime", "amount", "note"]
        raw_data["amount"] = raw_data["amount"].apply(lambda x: -x)
        raw_data["third_party_category"] = None
        raw_data = raw_data[
            ["datetime", "amount", "source", "third_party_category", "note"]
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["source"] + "_" + raw_data["note"]
        raw_data["note"] = raw_data["note"].apply(lambda s: s.replace(",", "."))
        raw_data["id"] = raw_data.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        return raw_data

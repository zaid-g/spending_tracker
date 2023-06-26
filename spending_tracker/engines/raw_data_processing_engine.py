import hashlib
import os
import shutil

import dateutil
import numpy as np
import pandas as pd

from spending_tracker.engines.data_validation_engine import DataValidationEngine


class RawDataProcessingEngine:

    """This engine is for
    1) Validating raw (downloaded) data integrity.
    2) Processing raw files from various supported accounts (e.g. Chase, Amazon) into
    files with a cleaned and unified format.

    If adding new method to support a new account/format, output should contain
    [datetime,amount,third_party_category,note,account,id] columns. Checkout the existing
    methods for more details.
    """

    def __init__(
        self,
        data_validation_engine: DataValidationEngine,
        root_data_folder_path: str,
        supported_accounts: dict,
    ):
        self.root_data_folder_path = root_data_folder_path.rstrip("/") + "/"
        self.raw_data_folder_path = self.root_data_folder_path + "raw/"
        self.processed_data_folder_path = self.root_data_folder_path + "processed/"
        self.data_validation_engine = data_validation_engine
        self.supported_accounts = supported_accounts
        self.create_folder_structure_if_not_exists()

    def read_raw_data_file_names(self) -> list:
        raw_data_file_names = []
        for file_name in os.listdir(self.raw_data_folder_path):
            if os.path.isfile(os.path.join(self.raw_data_folder_path, file_name)) and (
                file_name[0] != "."
            ):
                raw_data_file_names.append(file_name)
        return raw_data_file_names

    def create_folder_structure_if_not_exists(self):
        self.data_validation_engine.verify_path_not_file(self.root_data_folder_path)
        os.makedirs(self.root_data_folder_path, exist_ok=True)
        os.makedirs(self.raw_data_folder_path, exist_ok=True)
        # delete everything in processed_data_folder_path for idempotency
        # processed files re-created every run based on raw files.
        try:
            shutil.rmtree(self.processed_data_folder_path)
        except FileNotFoundError:
            pass
        os.makedirs(self.processed_data_folder_path, exist_ok=True)

    def process_raw_data_files(self) -> None:
        raw_data_file_names = self.read_raw_data_file_names()
        self.data_validation_engine.verify_raw_data_file_names_contain_date_range(
            raw_data_file_names
        )
        self.data_validation_engine.verify_raw_data_file_names_contain_one_account(
            raw_data_file_names
        )
        self.data_validation_engine.verify_account_raw_data_file_names_date_ranges(
            raw_data_file_names
        )
        for raw_data_file_name in raw_data_file_names:
            account = self.detect_account_in_raw_data_file_name(raw_data_file_name)
            raw_data_file_path = self.raw_data_folder_path + raw_data_file_name
            raw_data = pd.read_csv(raw_data_file_path)
            self.data_validation_engine.verify_raw_data_contains_correct_columns(
                raw_data, raw_data_file_path, account
            )
            raw_data["account"] = account
            # call function with the name of the account
            account_processing_method = getattr(self, account)
            processed_data = account_processing_method(
                raw_data, raw_data_file_path, raw_data_file_name
            )
            # processed data file name same as raw data file name
            self.data_validation_engine.verify_processed_data_bound_by_date_range(
                raw_data_file_name, processed_data
            )
            processed_data.to_csv(
                self.processed_data_folder_path + raw_data_file_name, index=False
            )

    def detect_account_in_raw_data_file_name(self, raw_data_file_name: str) -> str:
        for account in self.supported_accounts:
            if account in raw_data_file_name:
                return account

    @staticmethod
    def remove_non_numerical_chars(str_: str) -> float:
        """This is to convert e.g. '$14.83' to 14.83"""
        chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        list_ = list(str_)
        list_ = [c for c in list_ if c in chars]
        return float("".join(list_))

    def american_express_blue_cash_preferred_2022_1(
        self, raw_data, raw_data_file_path, raw_data_file_name
    ) -> pd.DataFrame:
        raw_data = raw_data.loc[:, ("Date", "Amount", "Description", "account")]
        raw_data.columns = ["datetime", "amount", "note", "account"]
        raw_data["third_party_category"] = None
        raw_data = raw_data[
            ["datetime", "amount", "account", "third_party_category", "note"]
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["account"] + "_" + raw_data["note"]
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

        def merge_debit_credit_columns(row):
            if np.isnan(row["Debit"]):
                return row["Credit"]
            else:
                return row["Debit"]

        raw_data["amount"] = raw_data.apply(
            lambda row: merge_debit_credit_columns(row), axis=1
        )
        raw_data = raw_data.loc[:, ("Date", "amount", "Description", "account")]
        raw_data.columns = ["datetime", "amount", "note", "account"]
        raw_data.loc[:, "third_party_category"] = None
        raw_data = raw_data[
            ["datetime", "amount", "account", "third_party_category", "note"]
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["account"] + "_" + raw_data["note"]
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
        return self.citi_double_cash_2022_1(
            raw_data, raw_data_file_path, raw_data_file_name
        )

    def amazon_refunds_2022_1(
        self, raw_data, raw_data_file_path, raw_data_file_name
    ) -> pd.DataFrame:
        raw_data["amount"] = raw_data["Refund Amount"].apply(
            lambda x: self.remove_non_numerical_chars(x)
        ) + raw_data["Refund Tax Amount"].apply(
            lambda x: self.remove_non_numerical_chars(x)
        )
        raw_data["amount"] = raw_data["amount"] * -1
        raw_data = raw_data.loc[
            :, ("Order Date", "amount", "Category", "Title", "account")
        ]
        raw_data.columns = [
            "datetime",
            "amount",
            "third_party_category",
            "note",
            "account",
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["account"] + "_" + raw_data["note"]
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
        raw_data = raw_data.loc[
            :, ("Order Date", "Item Total", "Category", "Title", "account")
        ]
        raw_data.columns = [
            "datetime",
            "amount",
            "third_party_category",
            "note",
            "account",
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["amount"] = raw_data["amount"].apply(
            lambda x: self.remove_non_numerical_chars(x)
        )
        raw_data["note"] = raw_data["account"] + "_" + raw_data["note"]
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
        raw_data = raw_data.loc[
            :, ("Post Date", "Description", "Category", "Amount", "account")
        ]
        raw_data.columns = [
            "datetime",
            "note",
            "third_party_category",
            "amount",
            "account",
        ]
        raw_data["amount"] = raw_data["amount"].apply(lambda x: -x)
        raw_data = raw_data[
            ["datetime", "amount", "account", "third_party_category", "note"]
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["account"] + "_" + raw_data["note"]
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
        raw_data = raw_data.loc[:, ("Posting Date", "Amount", "Description", "account")]
        raw_data.columns = ["datetime", "amount", "note", "account"]
        raw_data["amount"] = raw_data["amount"].apply(lambda x: -x)
        raw_data["third_party_category"] = None
        raw_data = raw_data[
            ["datetime", "amount", "account", "third_party_category", "note"]
        ]
        raw_data["datetime"] = raw_data["datetime"].apply(
            lambda datetime_string: dateutil.parser.parse(datetime_string)
        )
        raw_data["note"] = raw_data["account"] + "_" + raw_data["note"]
        raw_data["note"] = raw_data["note"].apply(lambda s: s.replace(",", "."))
        raw_data["id"] = raw_data.apply(
            lambda row: hashlib.sha256(str(row.values).encode("utf-8")).hexdigest()[
                0:30
            ],
            axis=1,
        )
        return raw_data

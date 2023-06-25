import datetime
import os
import re

import pandas as pd
from dateutil import parser


class DataValidationEngine:
    def __init__(self, supported_accounts: dict):
        self.supported_accounts = supported_accounts

    @staticmethod
    def verify_processed_data_folder_path_not_empty(folder_path):
        if len(os.listdir(folder_path)) == 0:
            raise FileNotFoundError(
                f"No files found in {folder_path} (no raw files were processed)"
            )

    @staticmethod
    def verify_path_not_file(root_data_folder_path) -> None:
        if os.path.exists(root_data_folder_path) and os.path.isfile(
            root_data_folder_path
        ):
            raise FileExistsError(
                f"Root data folder path {root_data_folder_path} "
                f"is a file. Must be folder."
            )

    def verify_processed_data_bound_by_date_range(
        self, processed_data_file_name, processed_data
    ):
        from_date = parser.parse(
            processed_data_file_name[0:4]
            + processed_data_file_name[5:7]
            + processed_data_file_name[8:10]
        )
        to_date = parser.parse(
            processed_data_file_name[14:18]
            + processed_data_file_name[19:21]
            + processed_data_file_name[22:24]
        )
        if any((processed_data["datetime"] < from_date)) or any(
            (processed_data["datetime"] > to_date)
        ):
            raise ValueError(
                f"Out of bound transaction "
                f"in processed data {processed_data_file_name}. "
                f"Please check date range of raw file/transactions."
            )

    def verify_raw_data_file_names_contain_one_account(
        self, raw_data_file_names
    ) -> None:
        for raw_data_file_name in raw_data_file_names:
            num_accounts_in_name = 0
            for supported_account in self.supported_accounts:
                if supported_account in raw_data_file_name.lower():
                    num_accounts_in_name += 1
            if num_accounts_in_name == 0:
                raise ValueError(
                    f"{raw_data_file_name} does not contain any supported "
                    f"account: {[acc for acc in self.supported_accounts]}.\n"
                    f"Cannot identify file"
                )
            elif num_accounts_in_name > 1:
                raise ValueError(
                    f"{raw_data_file_name} contains multiple accounts. "
                    f"Please fix name to contain only one of the supported "
                    f"accounts {[acc for acc in self.supported_accounts]}"
                )

    def verify_raw_data_contains_correct_columns(
        self, raw_data, raw_data_file_path, account
    ):
        if set(raw_data.columns) != set(self.supported_accounts[account]):
            raise ValueError(
                f"Invalid columns in {raw_data_file_path} based on account "
                f"{account}.\nCheck config.yaml for more details. "
                f"{raw_data.columns} != {self.supported_accounts[account]}"
            )

    def verify_account_raw_data_file_names_date_ranges(
        self, raw_data_file_names
    ) -> None:
        """Group the raw data file names by (supported) account names.
        For each group, make sure that the date ranges are valid and
        that there are no overlaps (to make sure no duplicate transactions).
        """
        for account in self.supported_accounts:
            account_raw_data_file_names = [
                f for f in raw_data_file_names if account in f
            ]
            account_raw_data_file_names.sort(
                key=lambda f: int(f[0:4] + f[5:7] + f[8:10])
            )
            self.verify_raw_data_file_name_contains_proper_date_ranges_for_each_account(
                account_raw_data_file_names
            )

    @staticmethod
    def verify_raw_data_file_name_contains_proper_date_ranges_for_each_account(
        account_raw_data_file_names,
    ) -> None:
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
                    f"Found file with from date greater than to date "
                    f"{account_raw_data_file_names[i]}"
                )
        # next verify no overlaps in date ranges
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
            if (from_date_next - to_date) < datetime.timedelta(days=1):
                raise ValueError(
                    f"Found overlaps in date range for "
                    f"{account_raw_data_file_names[i], account_raw_data_file_names[i+1]}"
                )

    def verify_raw_data_file_names_contain_date_range(
        self,
        raw_data_file_names,
    ) -> None:
        for raw_data_file_name in raw_data_file_names:
            if not self.contains_date_range(raw_data_file_name):
                raise ValueError(
                    f"No date range detected in file {raw_data_file_name}. "
                    f"Format is \nYYYY-MM-DD_to_YYYY-MM-DD_*"
                )

    @staticmethod
    def contains_date_range(file_name) -> bool:
        match_ = re.search(r"^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}", file_name)
        if match_ is None:
            return False
        return True

    @staticmethod
    def verify_no_duplicate_ids(df: pd.DataFrame) -> None:
        if len(df) != len(df.id.value_counts()):
            raise ValueError(
                f"Found duplicate ID(s):\n "
                f"{df[df.id.isin(df.id.value_counts()[df.id.value_counts() > 1].index)]}"
            )

    @staticmethod
    def verify_no_pattern_maps_to_more_than_one_category(pattern_category_map_list):
        patterns = [
            pattern_category[0] for pattern_category in pattern_category_map_list
        ]
        for pattern in patterns:
            mapped_categories = set()
            for i in range(len(pattern_category_map_list)):
                if pattern_category_map_list[i][0] == pattern:
                    mapped_categories.add(pattern_category_map_list[i][1])
            if len(mapped_categories) != 1:
                raise ValueError(
                    f"Found the same pattern **{pattern}** "
                    f"mapping to more than one category **{mapped_categories}**"
                )

    @staticmethod
    def verify_category_is_string_type(category) -> None:
        if pd.isna(category):
            return
        if type(category) != str:
            raise ValueError("Category must be string")

    @staticmethod
    def verify_pattern_matches_text(pattern, text, hide_text=False) -> None:
        text = text.lower()
        if pd.isna(pattern):
            return
        if re.compile(pattern).search(text) is None:
            if hide_text:
                raise ValueError("Pattern doesn't match text.")
            else:
                raise ValueError(
                    f"Pattern doesn't match text. Pattern: {pattern} --- Text: {text}"
                )

    @staticmethod
    def verify_all_historical_categorized_transactions_accounted_for_in_processed_data(
        historical_categorized_transactions, processed_data
    ) -> None:
        if not set(historical_categorized_transactions.id.values).issubset(
            processed_data.id.values
        ):
            missing_ids = set(historical_categorized_transactions.id.values) - set(
                processed_data.id.values
            )
            missing_transactions = historical_categorized_transactions[
                [historical_categorized_transactions.id.isin(missing_ids)]
            ]
            raise ValueError(
                f"Not all categorized transactions accounted for in processed "
                f"data folder:\n{missing_transactions}"
            )

    @staticmethod
    def verify_processed_data_columns(processed_data) -> None:
        if set(processed_data.columns) != {
            "id",
            "datetime",
            "amount",
            "account",
            "third_party_category",
            "note",
        }:
            raise ValueError("Processed data files invalid columns")

    @staticmethod
    def verify_categorized_transactions_columns(categorized_transactions) -> None:
        if set(categorized_transactions.columns) != {
            "id",
            "datetime",
            "amount",
            "account",
            "third_party_category",
            "note",
            "pattern",
            "category",
        }:
            raise ValueError("Categorized data files invalid columns")

    @staticmethod
    def verify_spend_amount_for_mapped_categories(
        spend_amount_by_category: pd.DataFrame,
    ) -> None:
        # TODO: DOCUMENT and understand and mby use regex because others might not have
        # the same categories. also write in readme
        return

import re
from dependency_injector import providers
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


class DataValidationEngine:
    def __init__(self):
        pass

    @staticmethod
    def contains_date_range(file_name) -> bool:
        match_ = re.search(r"^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}", file_name)
        if match_ == None:
            return False
        return True

    def verify_raw_data_file_names_contain_date_range_and_valid_account_name(
        self,
    ) -> None:
        for raw_data_file_name in raw_data_file_names:
            found_account = False
            for account in self.supported_accounts:
                if account in raw_data_file_name:
                    found_account = True
            if not found_account:
                # make sure all raw_data_file_names have a valid account name
                raise ValueError(
                    f"File {raw_data_file_name} does not match any existing account name {self.supported_accounts}"
                )
            if not self.contains_date_range(raw_data_file_name):
                # make sure all raw_data_file_names have date range
                raise ValueError(
                    f"No date range detected in file {file}. Format is \n^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}"
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

    def verify_raw_data_file_names_contain_proper_date_ranges_for_each_account(
        self,
    ) -> None:
        for account in self.supported_accounts:
            account_raw_data_file_names = [
                f for f in self.raw_data_file_names if account in f
            ]
            account_raw_data_file_names.sort(
                key=lambda f: int(f[0:4] + f[5:7] + f[8:10])
            )
            self.verify_raw_data_file_name_contains_proper_date_ranges_for_each_account(
                account_raw_data_file_names
            )

    def verify_no_duplicate_ids(self, df: pd.DataFrame) -> None:
        if len(df) != len(df.id.value_counts()):
            raise ValueError(
                f"Error: Found duplicate ID(s):\n {df[df.id.isin(df.id.value_counts()[ df.id.value_counts() > 1 ].index)]}"
            )

    @staticmethod
    def verify_no_pattern_maps_to_more_than_one_category(
        pattern_category_map_list
    ):
        patterns = sorted(
            list(set([pattern for pattern, _ in pattern_category_map_list]))
        )
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
            ), f"Error: Found the same pattern **{pattern}** mapping to more than one category **{mapped_categories}**"

    @staticmethod
    def verify_category_is_string_type(category) -> None:
        if pd.isna(category):
            return
        if type(category) != str:
            raise ValueError("category must be string")

    @staticmethod
    def verify_pattern_matches_text(self, pattern, text) -> None:
        text = text.lower()
        if pd.isna(pattern):
            return
        if re.compile(pattern).search(text) == None:
            raise ValueError(f"Error: found pattern that doesn't match note (text). Pattern: {pattern} --- Text: {text}")

    def verify_all_historical_data_accounted_for_in_processed_data(
        self, hist_df, processed_df
    ) -> None:
        if not set(hist_df.id.values).issubset(df.id.values):
            raise ValueError(
                "Error: not all historical transactions accounted for in processed or raw csvs"
            )

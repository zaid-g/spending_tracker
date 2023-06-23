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
    def __init__(self, supported_accounts: dict):
        self.supported_accounts = supported_accounts

    def verify_raw_data_file_names_contain_only_single_account(
        self, raw_data_file_names
    ) -> None:
        for raw_data_file_name in raw_data_file_names:
            num_accounts_in_name = 0
            for supported_account in self.supported_accounts:
                if supported_account in raw_data_file_name.lower():
                    num_accounts_in_name += 1
            if num_accounts_in_name != 0:
                raise ValueError(
                    f"{raw_data_file_name} does not contain any supported account. Cannot identify file"
                )
            elif num_accounts_in_name > 1:
                raise ValueError(
                    f"{raw_data_file_name} contains multiple accounts. Please fix name to contain only one of the supported accounts {[acc for acc in self.supported_accounts]}"
                )

    def verify_raw_data_file_names_contain_proper_date_ranges_for_each_account(
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
                    f"Error: Found file with from date greater than to date {account_raw_data_file_names[i]}"
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
            if not (from_date_next - to_date < datetime.timedelta(days=1)):
                raise ValueError(
                    f"Error: Found overlaps in date range for {account_raw_data_file_names[i], account_raw_data_file_names[i+1]}"
                )

    def verify_raw_data_file_names_contain_date_range(
        self,
    ) -> None:
        for raw_data_file_name in raw_data_file_names:
            if not self.contains_date_range(raw_data_file_name):
                raise ValueError(
                    f"No date range detected in file {file}. Format is \n^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}"
                )

    @staticmethod
    def contains_date_range(file_name) -> bool:
        match_ = re.search(r"^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}", file_name)
        if match_ == None:
            return False
        return True

    @staticmethod
    def verify_no_duplicate_ids(df: pd.DataFrame) -> None:
        if len(df) != len(df.id.value_counts()):
            raise ValueError(
                f"Error: Found duplicate ID(s):\n {df[df.id.isin(df.id.value_counts()[ df.id.value_counts() > 1 ].index)]}"
            )

    @staticmethod
    def verify_no_pattern_maps_to_more_than_one_category(pattern_category_map_list):
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
    def verify_pattern_matches_text(pattern, text) -> None:
        text = text.lower()
        if pd.isna(pattern):
            return
        if re.compile(pattern).search(text) == None:
            raise ValueError(
                f"Error: found pattern that doesn't match note (text). Pattern: {pattern} --- Text: {text}"
            )

    @staticmethod
    def verify_all_historical_categorized_transactions_accounted_for_in_processed_data(
        historical_categorized_transactions, processed_data
    ) -> None:
        if not set(historical_categorized_transactions.id.values).issubset(
            processed_data.id.values
        ):
            raise ValueError(
                "Error: not all categorized transactions accounted for in processed data folder"
            )

    @staticmethod
    def verify_processed_data_columns(processed_data) -> None:
        if set(processed_data.columns) != {
            "id",
            "datetime",
            "amount",
            "source",
            "third_party_category",
            "note",
        }:
            raise ValueError(f"processed data files invalid columns")

    @staticmethod
    def verify_spend_amount_for_mapped_categories(
        spend_amount_by_category: pd.DataFrame,
    ) -> None:
        # TODO: DOCUMENT and understand and mby use regex because others might not have
        # the same categories. also write in readme

        # amazon
        mapped_amazon_total = spend_amount_by_category["mapped/amazon"]
        amazon_total = sum(
            df[
                (df["source"] == "amazon_items") | (df["source"] == "amazon_refunds")
            ].amount
        )
        print(
            f"\n\nCategory 'mapped/amazon' total = {mapped_amazon_total}, total amazon payments = {amazon_total}"
        )

        # venmo
        mapped_venmo_total = spend_amount_by_category["mapped/venmo"]
        venmo_total = sum(df[df["source"] == "venmo"].amount)
        print(
            f"\n\nCategory 'mapped/venmo' total = {mapped_venmo_total}, total venmo payments = {venmo_total}"
        )

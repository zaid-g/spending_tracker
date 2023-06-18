import pandas as pd
import glob
import os
import numpy as np
import re


class CategorizationEngine:

    """This engine is for
    1) Loading historically categorized and new data
    2) TODO
    """

    def __init__(self, root_data_folder_path: str):
        self.root_data_folder_path = root_data_folder_path
        self.cleaned_data_folder_path = self.root_data_folder_path + "cleaned/"
        self.cleaned_data_file_names = self.read_cleaned_data_file_names()
        self.cleaned_data = self.load_cleaned_data()
        self.historical_categorized_data_file_path = self.root_data_folder_path + "history.csv"
        self.historical_categorized_data = self.load_categorized_data()

    def read_cleaned_data_file_names(self):
        return glob.glob(os.path.join(self.cleaned_data_folder_path, "*.csv"))

    def load_cleaned_data(self) -> pd.DataFrame:
        df_list = []
        for cleaned_csv_file_name in self.cleaned_data_file_names:
            df = pd.read_csv(
                cleaned_csv_file_name,
                index_col=None,
                header=0,
                parse_dates=["datetime"],
            )
            df_list.append(df)
        cleaned_data = pd.concat(df_list, axis=0, ignore_index=True)
        if set(cleaned_data.columns) != {
                "id",
                "datetime",
                "amount",
                "source",
                "third_party_category",
                "note",
                }: 
            raise ValueError(f"Cleaned data files invalid columns")
        return cleaned_data

    def verify_no_duplicate_transactions_in_cleaned_data(self) -> None:
        if len(self.cleaned_data) != len(
            self.cleaned_data.id.value_counts()
            ):
            raise ValueError(f"Error: Found duplicate ID(s) in cleaned files:\n {self.cleaned_data[self.cleaned_data.id.isin(self.cleaned_data.id.value_counts()[ self.cleaned_data.id.value_counts() > 1 ].index)]}")

    def load_historical_categorized_data(self) -> pd.DataFrame:
        # first read entire df including written history
        if os.path.isfile(self.historical_categorized_data_file_path) is False:
            historical_categorized_data = pd.DataFrame(
                columns=[
                    "id",
                    "datetime",
                    "amount",
                    "source",
                    "third_party_category",
                    "note",
                    "category",
                    "pattern",
                ]
            )
        else:
            historical_categorized_data = pd.read_csv(historical_categorized_csv_path, parse_dates=["datetime"])
        return historical_categorized_data

    def verify_historical_categorized_data_pattern_matches_text() -> None:
        # assert all patterns in historical file indeed do match text of that transaction
        self.historical_categorized_data.apply(
            lambda row: self.make_sure_pattern_matches_text(row["pattern"], row["note"]), axis=1
        )


    def make_sure_no_pattern_maps_to_more_than_one_category(
        self, pattern_category_map_list
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

    def make_sure_category_dtype(self, category):
        if pd.isna(category):
            return
        assert type(category) == str, "category must be string"

    @staticmethod
    def make_sure_pattern_matches_text(self, pattern, text) -> None:
        text = text.lower()
        if pd.isna(pattern):
            return
        assert (
            re.compile(pattern).search(text) != None
        ), f"Error: found pattern that doesn't match note (text). Pattern: {pattern} --- Text: {text}"

    def extract_patterns_categories_from_history(self, hist_df):
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

    def get_matched_pattern(self, text, pattern_category_map_list):
        """Returns longest pattern that matches text"""
        matched_patterns = []
        for pattern, _ in pattern_category_map_list:
            if re.compile(pattern).search(text.lower()) != None:
                matched_patterns.append(pattern)
        if len(matched_patterns) == 0:
            return
        return max(matched_patterns, key=len)

    def get_category_from_pattern(self, pattern, pattern_category_map_dict):
        if pattern == None:
            return None
        else:
            return pattern_category_map_dict[pattern]

import pandas as pd
from spending_tracker.engines.data_validation_engine import DataValidationEngine
import glob
import os
import numpy as np
import re


class CategorizationEngine:

    """This engine is for
    1) Loading historically categorized and new data
    2) TODO
    """

    def __init__(
        self, data_validation_engine: DataValidationEngine, root_data_folder_path: str
    ):
        self.root_data_folder_path = root_data_folder_path
        self.processed_data_folder_path = self.root_data_folder_path + "processed/"
        self.processed_data_file_names = self.read_processed_data_file_names()
        self.processed_data = self.load_processed_data()
        self.historical_categorized_data_file_path = (
            self.root_data_folder_path + "history.csv"
        )
        self.historical_categorized_data = self.load_historical_categorized_data()
        (
            self.pattern_category_map_list,
            self.pattern_category_map_dict,
            self.all_categories,
            self.all_patterns,
        ) = self.get_all_patterns_categories_from_historical_categorized_data()

        # make sure no pattern maps to more than one category
        verify_no_pattern_maps_to_more_than_one_category(pattern_category_map_list)
        # assert all categories are valid strings in historical file
        hist_df["category"].apply(
            lambda category: verify_category_is_string_type(category)
        )
        # make sure no ids are duplicated
        assert len(hist_df) == len(
            hist_df.id.value_counts()
        ), "Error: found duplicate IDs in historical file"

        # apply patterns on new data
        df["pattern"] = df["note"].apply(
            lambda text: get_matched_pattern(text, pattern_category_map_list)
        )
        df["category"] = df["pattern"].apply(
            lambda pattern: get_category_from_pattern(
                pattern, pattern_category_map_dict
            )
        )

        # update df to include history
        # first remove df rows that exist in historical df
        df = df[~df.id.isin(hist_df.id.values)]
        df = df.sort_values(["datetime", "note"], ascending=False, ignore_index=True)
        df = pd.concat([df, hist_df], axis=0, ignore_index=True)
        df.index = range(len(df) - 1, -1, -1)
        # make sure no ids are duplicated
        assert len(hist_df) == len(
            hist_df.id.value_counts()
        ), "Error: found duplicate IDs in concatenated history + recent file (really weird if you get this error)"
        self.run_tui()

    def read_processed_data_file_names(self):
        return glob.glob(os.path.join(self.processed_data_folder_path, "*.csv"))

    def load_processed_data(self) -> pd.DataFrame:
        df_list = []
        for processed_csv_file_name in self.processed_data_file_names:
            df = pd.read_csv(
                processed_csv_file_name,
                index_col=None,
                header=0,
                parse_dates=["datetime"],
            )
            df_list.append(df)
        processed_data = pd.concat(df_list, axis=0, ignore_index=True)
        if set(processed_data.columns) != {
            "id",
            "datetime",
            "amount",
            "source",
            "third_party_category",
            "note",
        }:
            raise ValueError(f"Cleaned data files invalid columns")
        return processed_data

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
            historical_categorized_data = pd.read_csv(
                self.historical_categorized_data_file_path, parse_dates=["datetime"]
            )
        return historical_categorized_data

    def verify_historical_categorized_data_pattern_matches_text() -> None:
        # assert all patterns in historical file indeed do match text of that transaction
        self.historical_categorized_data.apply(
            lambda row: self.make_sure_pattern_matches_text(
                row["pattern"], row["note"]
            ),
            axis=1,
        )

    def get_all_patterns_categories_from_historical_categorized_data(
        self, historical_categorized_data
    ):
        """returns tuple and dict objects"""
        pattern_category_map_list = list(
            set(
                [
                    (row["pattern"], row["category"])
                    for index, row in self.historical_categorized_data.iterrows()
                    if not pd.isna(row["pattern"])
                ]
            )
        )
        pattern_category_map_dict = dict(pattern_category_map_list)
        all_categories = [
            i
            for i in self.historical_categorized_data["category"].unique()
            if not pd.isna(i)
        ]
        all_patterns = [
            i
            for i in self.historical_categorized_data["pattern"].unique()
            if not pd.isna(i)
        ]
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

    def run_categorization_tui(self):
        # time to ask user to confirm or override
        # sort categories and patterns for visual display
        while True:
            all_categories.sort()
            all_patterns.sort()

            print()
            print(
                df[
                    [
                        "note",
                        "category",
                        "pattern",
                        "amount",
                        "datetime",
                        "source",
                        "third_party_category",
                    ]
                ]
            )

            while True:
                try:
                    if "transaction_index" in locals():
                        last_transaction_index = transaction_index
                        print(f"\nLast transaction index: {transaction_index}")
                    transaction_index = input(
                        "Select row you would like to categorize.\nEnter -1 if this looks good.\nEnter -2 for breakpoint.\nEnter -3 to quit without saving.\n"
                    )
                    if transaction_index == "":
                        if "last_transaction_index" in locals():
                            transaction_index = last_transaction_index + 1
                        else:
                            transaction_index = 0
                    transaction_index = int(transaction_index)
                    if transaction_index >= 0:
                        df.loc[transaction_index]
                    break
                except KeyboardInterrupt:
                    exit()
                except:
                    print("Not an integer value or out of range...")
            if transaction_index == -1:
                break
            if transaction_index == -2:
                ipdb.set_trace()
                print("Exiting without saving")
                break
            if transaction_index == -3:
                exit()
            print("\n      ***** Transaction Details ******         ")
            print(
                df.loc[transaction_index][
                    [
                        "datetime",
                        "amount",
                        "source",
                        "third_party_category",
                        "pattern",
                        "category",
                    ]
                ]
            )
            print()
            print(df.loc[transaction_index, "note"])
            if pd.notna(df.loc[transaction_index, "category"]):
                print(
                    f'\n- This transaction is already categorized as **{df.loc[transaction_index, "category"]}** and matches pattern **{df.loc[transaction_index, "pattern"]}**'
                )
            print("\n      ***** All Categories ******         ")
            for i in range(len(all_categories)):
                print(f"{i}: {all_categories[i]}")
            inputted_category = input(
                f"\nCategorize this transaction by typing in category or selecting index of pre-existing category (enter to skip):\n"
            )
            if inputted_category != "":
                if inputted_category.isdigit():
                    inputted_category = all_categories[int(inputted_category)]
                    df.loc[transaction_index, "category"] = inputted_category
                else:
                    inputted_category = inputted_category.strip("/").lower()
                    df.loc[transaction_index, "category"] = inputted_category
                while True:
                    print("\n      ***** All Patterns ******         \n")
                    for i in range(len(all_patterns)):
                        print(f"{i}: {all_patterns[i]}")
                    inputted_pattern = input(
                        f"\nAdd a pattern for category **{inputted_category}** based on this transaction. Assume text is lower-cased. (enter to skip)\n\n{df.loc[transaction_index, 'note']}\n"
                    )
                    if inputted_pattern == "":
                        if inputted_category not in all_categories:
                            all_categories.append(inputted_category)
                        break
                    try:
                        if inputted_pattern.isdigit():
                            inputted_pattern = all_patterns[int(inputted_pattern)]
                        verify_pattern_matches_text(
                            inputted_pattern, df.loc[transaction_index, "note"]
                        )
                        if (
                            inputted_pattern,
                            inputted_category,
                        ) not in pattern_category_map_list:
                            pattern_category_map_list_copy = deepcopy(
                                pattern_category_map_list
                            )
                            pattern_category_map_list_copy.append(
                                (inputted_pattern, inputted_category)
                            )
                            verify_no_pattern_maps_to_more_than_one_category(
                                pattern_category_map_list_copy
                            )
                            pattern_category_map_list.append(
                                (inputted_pattern, inputted_category)
                            )
                            pattern_category_map_dict[
                                inputted_pattern
                            ] = inputted_category
                            df.loc[transaction_index, "pattern"] = inputted_pattern
                            if inputted_category not in all_categories:
                                all_categories.append(inputted_category)
                            if inputted_pattern not in all_patterns:
                                all_patterns.append(inputted_pattern)
                        break
                    except:
                        print("Error: inputted pattern does not match text (note)")

    def write_categorized_data(self):
        print("Writing history.")
        df[
            [
                "id",
                "datetime",
                "amount",
                "source",
                "third_party_category",
                "note",
                "preselected_category",
                "pattern",
                "category",
            ]
        ].to_csv(data_root_path + "history.csv", index=False)

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
        self.historical_categorized_data_file_path = (
            self.root_data_folder_path + "history.csv"
        )
        (
            self.data_to_categorize,
            self.pattern_category_map_list,
            self.pattern_category_map_dict,
            self.all_categories,
            self.all_patterns,
        ) = self.load_data_to_categorize()

    def load_historical_categorized_data(self) -> pd.DataFrame:
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
        # make sure all categories are valid strings in historical file
        historical_categorized_data["category"].apply(
            lambda category: self.data_validation_engine.verify_category_is_string_type(
                category
            )
        )
        # make sure no duplicate ids (transactions) in historical data
        self.data_validation_engine.verify_no_duplicate_ids(historical_categorized_data)
        # make sure pattern matches note in historical data
        historical_categorized_data.apply(
            lambda row: self.data_validation_engine.verify_pattern_matches_text(
                row["pattern"], row["note"]
            ),
            axis=1,
        )
        return historical_categorized_data

    def get_all_patterns_categories_from_historical_categorized_data(
        self, historical_categorized_data
    ):
        """returns tuple and dict objects"""
        pattern_category_map_list = list(
            set(
                [
                    (row["pattern"], row["category"])
                    for index, row in historical_categorized_data.iterrows()
                    if not pd.isna(row["pattern"])
                ]
            )
        )
        pattern_category_map_dict = dict(pattern_category_map_list)
        all_categories = [
            i
            for i in historical_categorized_data["category"].unique()
            if not pd.isna(i)
        ]
        all_patterns = [
            i for i in historical_categorized_data["pattern"].unique() if not pd.isna(i)
        ]
        # make sure no pattern maps to more than one category
        self.data_validation_engine.verify_no_pattern_maps_to_more_than_one_category(
            pattern_category_map_list
        )
        return (
            pattern_category_map_list,
            pattern_category_map_dict,
            all_categories,
            all_patterns,
        )

    def load_processed_data(self) -> pd.DataFrame:
        # get processed data file names
        processed_data_file_paths = glob.glob(
            os.path.join(self.processed_data_folder_path, "*.csv")
        )
        # load all processed data
        df_list = []
        for processed_data_file_path in processed_data_file_paths:
            df = pd.read_csv(
                processed_data_file_path,
                index_col=None,
                header=0,
                parse_dates=["datetime"],
            )
            df_list.append(df)
        processed_data = pd.concat(df_list, axis=0, ignore_index=True)
        # categorize processed data using historically created patterns
        self.categorize_processed_data_using_historical_patterns()
        # make sure no duplicate ids in processed data
        self.data_validation_engine.verify_no_duplicate_ids(processed_data)
        # make sure correct columns in processed data
        if set(processed_data.columns) != {
            "id",
            "datetime",
            "amount",
            "source",
            "third_party_category",
            "note",
        }:
            raise ValueError(f"processed data files invalid columns")
        return processed_data

    def categorize_processed_data_using_historical_patterns(processed_data) -> None:
        processed_data.loc[:, "pattern"] = processed_data["note"].apply(
            lambda text: self.get_longest_pattern_that_matches_text(
                text, self.pattern_category_map_list
            )
        )
        processed_data.loc[:, "category"] = processed_data["pattern"].apply(
            lambda pattern: self.get_category_from_pattern(
                pattern, self.pattern_category_map_dict
            )
        )

    def get_longest_pattern_that_matches_text(self, text, pattern_category_map_list):
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

    def load_data_to_categorize(self) -> tuple:
        # load historically categorized data
        historical_categorized_data = self.load_historical_categorized_data()
        # get mapped patterns & categories from historically categorized data
        (
            pattern_category_map_list,
            pattern_category_map_dict,
            all_categories,
            all_patterns,
        ) = self.get_all_patterns_categories_from_historical_categorized_data(
            historical_categorized_data
        )
        # load processed data
        processed_data = self.load_processed_data(historical_categorized_data.id)
        # make sure no missing rows from processed data
        self.data_validation_engine.verify_all_historical_data_accounted_for_in_processed_data(
            historical_categorized_data, processed_data
        )
        # discard processed data that was already historically categorized
        # and keep new uncategorized processed data that we want to categorize
        uncategorized_processed_data = processed_data[
            ~processed_data.id.isin(self.historical_categorized_data.id.values)
        ].sort_values(["datetime", "note"], ascending=False, ignore_index=True)
        # concatenate new uncategorized processed data with historical categorized data
        data_to_categorize = pd.concat(
            [uncategorized_processed_data, historical_categorized_data],
            axis=0,
            ignore_index=True,
        )
        data_to_categorize.index = range(len(data_to_categorize) - 1, -1, -1)
        # make sure no ids are duplicated
        self.data_validation_engine.verify_no_duplicate_ids(data_to_categorize)
        return (
            data_to_categorize,
            pattern_category_map_list,
            pattern_category_map_dict,
            all_categories,
            all_patterns,
        )

    def run_categorization_TUI(self):
        # time to ask user to confirm or override
        # sort categories and patterns for visual display
        while True:
            self.all_categories.sort()
            self.all_patterns.sort()
            print()
            print(
                self.data_to_categorize[
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
                        "Select row you would like to categorize.\nEnter `s` to save and quit if this looks good.\nPress Enter to move to categorize next transaction.\n"
                    )
                    if transaction_index == "":
                        if "last_transaction_index" in locals():
                            transaction_index = last_transaction_index + 1
                        else:
                            transaction_index = 0
                    elif transaction_index == "s":
                        transaction_index = -1
                    transaction_index = int(transaction_index)
                    if transaction_index >= 0:
                        self.data_to_categorize.loc[transaction_index]
                    break
                except KeyboardInterrupt:
                    exit()
                except:
                    print("Invalid input. Please try again.")
            if transaction_index == -1:
                break
            print("\n      ***** Transaction Details ******         ")
            print(
                self.data_to_categorize.loc[transaction_index][
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
            print(self.data_to_categorize.loc[transaction_index, "note"])
            if pd.notna(self.data_to_categorize.loc[transaction_index, "category"]):
                print(
                    f'\n- This transaction is already categorized as **{self.data_to_categorize.loc[transaction_index, "category"]}** and matches pattern **{self.data_to_categorize.loc[transaction_index, "pattern"]}**'
                )
            print("\n      ***** All Categories ******         ")
            for i in range(len(self.all_categories)):
                print(f"{i}: {self.all_categories[i]}")
            inputted_category = input(
                f"\nCategorize this transaction by typing in category or selecting index of pre-existing category (enter to skip):\n"
            )
            if inputted_category != "":
                if inputted_category.isdigit():
                    inputted_category = self.all_categories[int(inputted_category)]
                    self.data_to_categorize.loc[
                        transaction_index, "category"
                    ] = inputted_category
                else:
                    inputted_category = inputted_category.strip("/").lower()
                    self.data_to_categorize.loc[
                        transaction_index, "category"
                    ] = inputted_category
                while True:
                    print("\n      ***** All Patterns ******         \n")
                    for i in range(len(self.all_patterns)):
                        print(f"{i}: {self.all_patterns[i]}")
                    inputted_pattern = input(
                        f"\nAdd a pattern for category **{inputted_category}** based on this transaction. Assume text is lower-cased. (enter to skip)\n\n{self.data_to_categorize.loc[transaction_index, 'note']}\n"
                    )
                    if inputted_pattern == "":
                        if inputted_category not in self.all_categories:
                            self.all_categories.append(inputted_category)
                        break
                    try:
                        if inputted_pattern.isdigit():
                            inputted_pattern = all_patterns[int(inputted_pattern)]
                        self.data_validation_engine.verify_pattern_matches_text(
                            inputted_pattern,
                            self.data_to_categorize.loc[transaction_index, "note"],
                        )
                        if (
                            inputted_pattern,
                            inputted_category,
                        ) not in self.pattern_category_map_list:
                            pattern_category_map_list_copy = deepcopy(
                                self.pattern_category_map_list
                            )
                            pattern_category_map_list_copy.append(
                                (inputted_pattern, inputted_category)
                            )
                            verify_no_pattern_maps_to_more_than_one_category(
                                pattern_category_map_list_copy
                            )
                            self.pattern_category_map_list.append(
                                (inputted_pattern, inputted_category)
                            )
                            self.pattern_category_map_dict[
                                inputted_pattern
                            ] = inputted_category
                            self.data_to_categorize.loc[
                                transaction_index, "pattern"
                            ] = inputted_pattern
                            if inputted_category not in all_categories:
                                self.all_categories.append(inputted_category)
                            if inputted_pattern not in all_patterns:
                                self.all_patterns.append(inputted_pattern)
                        break
                    except:
                        print("Error: inputted pattern does not match text (note)")

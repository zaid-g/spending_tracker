import glob
import pprint
import cmd
import os
import re
from copy import deepcopy

import pandas as pd

from spending_tracker.engines.data_validation_engine import DataValidationEngine


class CategorizationEngine:

    """This engine is for
    1) Loading historically categorized and new transactions
    2) categorizing new and/or recategorizing old transactions using
    a terminal user interface (TUI)
    """

    def __init__(
        self, root_data_folder_path: str, data_validation_engine: DataValidationEngine
    ):
        self.root_data_folder_path = root_data_folder_path.rstrip("/") + "/"
        self.processed_data_folder_path = self.root_data_folder_path + "processed/"
        self.data_validation_engine = data_validation_engine
        self.historical_categorized_transactions_file_path = (
            self.root_data_folder_path + "categorized_transactions.csv"
        )
        # print options
        self.terminal_size = os.get_terminal_size()

    def load_historical_categorized_transactions(self) -> pd.DataFrame:
        """Load user-categorized transactions from previous runs"""
        if os.path.isfile(self.historical_categorized_transactions_file_path) is False:
            historical_categorized_transactions = pd.DataFrame(
                columns=[
                    "id",
                    "datetime",
                    "amount",
                    "account",
                    "third_party_category",
                    "note",
                    "pattern",
                    "category",
                ]
            )
        else:
            historical_categorized_transactions = pd.read_csv(
                self.historical_categorized_transactions_file_path,
                parse_dates=["datetime"],
            )
        # validate columns
        self.data_validation_engine.verify_categorized_transactions_columns(
            historical_categorized_transactions
        )
        # make sure all categories are valid strings in historical file
        historical_categorized_transactions["category"].apply(
            lambda category: self.data_validation_engine.verify_category_format(
                category
            )
        )
        # make sure no duplicate ids (transactions) in historical data
        self.data_validation_engine.verify_no_duplicate_ids(
            historical_categorized_transactions
        )
        # make sure pattern matches note in historical data
        historical_categorized_transactions.apply(
            lambda row: self.data_validation_engine.verify_pattern_matches_text(
                row["pattern"], row["note"]
            ),
            axis=1,
        )
        return historical_categorized_transactions

    def get_all_patterns_categories_from_historical_categorized_transactions(
        self, historical_categorized_transactions: pd.DataFrame
    ) -> tuple[list, dict, list, list]:
        """Extracts patterns and categories and returns in multiple formats"""
        pattern_category_map_list = list(
            set(
                [
                    (row["pattern"], row["category"])
                    for index, row in historical_categorized_transactions.iterrows()
                    if not pd.isna(row["pattern"])
                ]
            )
        )
        pattern_category_map_dict = dict(pattern_category_map_list)
        all_categories = [
            i
            for i in historical_categorized_transactions["category"].unique()
            if not pd.isna(i)
        ]
        all_patterns = [
            i
            for i in historical_categorized_transactions["pattern"].unique()
            if not pd.isna(i)
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

    def load_processed_data(
        self,
    ) -> pd.DataFrame:
        """Loads all data processed from all raw files in the /raw folder"""
        self.data_validation_engine.verify_processed_data_folder_path_not_empty(
            self.processed_data_folder_path
        )
        # get processed data file names
        processed_data_file_paths = glob.glob(
            os.path.join(self.processed_data_folder_path, "*.[cC][sS][vV]")
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
        # make sure no duplicate ids in processed data
        self.data_validation_engine.verify_no_duplicate_ids(processed_data)
        return processed_data

    def categorize_data_using_pattern_category_map(
        self, data: pd.DataFrame, indices=None
    ) -> None:
        """Uses patterns user created from seen data to categorize new transactions"""
        if indices is None:
            indices = data.index
        data.loc[indices, "pattern"] = data.loc[indices, "note"].apply(
            lambda text: self.get_longest_pattern_that_matches_text(
                text, self.pattern_category_map_list
            )
        )
        data.loc[indices, "category"] = data.loc[indices, "pattern"].apply(
            lambda pattern: self.get_category_from_pattern(
                pattern, self.pattern_category_map_dict
            )
        )

    def get_longest_pattern_that_matches_text(
        self, text, pattern_category_map_list
    ) -> str:
        """Returns longest pattern that matches text"""
        matched_patterns = []
        for pattern, _ in pattern_category_map_list:
            if re.compile(pattern).search(text.lower()) is not None:
                matched_patterns.append(pattern)
        if len(matched_patterns) == 0:
            return
        return max(matched_patterns, key=len)

    def get_category_from_pattern(
        self, pattern: str, pattern_category_map_dict: dict
    ) -> str:
        if pattern is None:
            return None
        else:
            return pattern_category_map_dict[pattern]

    def load_data_to_categorize(self) -> None:
        """Loads and sets as class attributes the transactions to categorize by user,
        and the previously created user patterns for auto categorization using regex"""
        # load historically categorized data
        historical_categorized_transactions = (
            self.load_historical_categorized_transactions()
        )
        # get mapped patterns & categories from historically categorized data
        (
            self.pattern_category_map_list,
            self.pattern_category_map_dict,
            self.all_categories,
            self.all_patterns,
        ) = self.get_all_patterns_categories_from_historical_categorized_transactions(
            historical_categorized_transactions
        )
        # load processed data
        processed_data = self.load_processed_data()
        # categorize processed data using historically created patterns
        self.categorize_data_using_pattern_category_map(processed_data)
        # make sure no missing rows from processed data
        self.data_validation_engine.verify_all_historical_categorized_transactions_accounted_for_in_processed_data(
            historical_categorized_transactions, processed_data
        )
        # discard processed data that was already historically categorized (seen) and
        # keep new uncategorized processed data that user wants to categorize
        unseen_processed_data = processed_data[
            ~processed_data.id.isin(historical_categorized_transactions.id.values)
        ]
        # concatenate new uncategorized processed data with historical categorized data
        unseen_processed_data["seen"] = False
        historical_categorized_transactions["seen"] = True
        self.transactions_to_categorize = pd.concat(
            [unseen_processed_data, historical_categorized_transactions],
            axis=0,
            ignore_index=True,
        ).sort_values(by=["datetime", "amount", "note", "id"])
        self.transactions_to_categorize.index = range(
            len(self.transactions_to_categorize) - 1, -1, -1
        )
        # make sure no ids are duplicated
        self.data_validation_engine.verify_no_duplicate_ids(
            self.transactions_to_categorize
        )

    def run_categorization_TUI(self):
        """Terminal user interface prompting user to categorize new transactions"""
        # time to ask user to confirm or override
        # sort categories and patterns for visual display
        transaction_index = -1
        while True:
            # sort categories and patterns alphabetically
            self.all_categories.sort()
            self.all_patterns.sort()
            # print transactions to categorize in readable format
            self.print_transactions_to_categorize()
            transaction_index = self.get_user_input_for_transaction_index(
                transaction_index
            )
            if transaction_index == -1:  # user selected Save and quit option
                self.save_categorized_transactions(self.transactions_to_categorize)
                break
            # else user selected a transaction by its (dataframe) index
            self.print_transaction_details(
                self.transactions_to_categorize.loc[transaction_index]
            )
            self.print_all_categories()
            # prompt user for category
            inputted_category = self.get_user_input_for_category()
            if inputted_category != "":  # skip to start if user pressed Enter.
                self.transactions_to_categorize.loc[
                    transaction_index, "category"
                ] = inputted_category
                self.transactions_to_categorize.loc[
                    transaction_index, "pattern"
                ] = None  # clear pattern if user overrides category
                # tag transaction as seen
                self.transactions_to_categorize.loc[transaction_index, "seen"] = True
                if inputted_category != None:  # i.e. user did not clear category
                    # set new category if doesn't exist
                    if inputted_category not in self.all_categories:
                        self.all_categories.append(inputted_category)
                    inputted_pattern = self.get_user_input_for_pattern(
                        self.transactions_to_categorize.loc[transaction_index],
                        inputted_category,
                    )
                    if inputted_pattern != "":  # skip to start if user pressed Enter.
                        if (
                            inputted_pattern,
                            inputted_category,
                        ) not in self.pattern_category_map_list:
                            # if new pattern -> cat mapping, add it
                            if inputted_pattern not in self.all_patterns:
                                self.all_patterns.append(inputted_pattern)
                            self.pattern_category_map_list.append(
                                (inputted_pattern, inputted_category)
                            )
                            self.pattern_category_map_dict[
                                inputted_pattern
                            ] = inputted_category
                            # tag the transaction with the pattern
                            self.transactions_to_categorize.loc[
                                transaction_index, "pattern"
                            ] = inputted_pattern
                            # apply all pattern including new on unseen data
                            self.categorize_data_using_pattern_category_map(
                                self.transactions_to_categorize,
                                self.transactions_to_categorize["seen"] == False,
                            )

    def get_user_input_for_transaction_index(self, last_transaction_index=-1) -> int:
        while True:
            try:
                transaction_index = input(
                    "\nSelect row you would like to categorize, enter `s` "
                    f"to save and quit if this looks good, or press "
                    f"Enter to categorize next transaction.\n"
                )
                if transaction_index == "":
                    transaction_index = last_transaction_index + 1
                    if transaction_index >= len(self.transactions_to_categorize):
                        print("\n\n *** Reached end of list *** \n\n")
                        return last_transaction_index
                elif transaction_index == "s":
                    transaction_index = -1
                transaction_index = int(transaction_index)
                if transaction_index >= 0:
                    self.transactions_to_categorize.loc[
                        transaction_index
                    ]  # to catch if out of bounds
                break
            except KeyboardInterrupt:
                print("Exiting without saving")
                exit()
            except Exception as e:
                print(f"\n❌❌❌❌Invalid input. Please try again. Error: {e}")
        return transaction_index

    def print_transactions_to_categorize(self):
        # truncate columns for terminal
        transactions_to_categorize_print = self.transactions_to_categorize.copy().fillna('-')[
            [
                "note",
                "category",
                "pattern",
                "amount",
                "datetime",
                "account",
                "third_party_category",
            ]
        ]
        transactions_to_categorize_print["third_party_category"] = (
            transactions_to_categorize_print["third_party_category"]
            .apply(lambda str_: self.truncate_string_for_print(str_, 15))
        )
        transactions_to_categorize_print["account"] = (
            transactions_to_categorize_print["account"]
            .apply(lambda str_: self.truncate_string_for_print(str_, 15))
        )
        transactions_to_categorize_print["pattern"] = (
            transactions_to_categorize_print["pattern"]
            .apply(lambda str_: self.truncate_string_for_print(str_, 15))
        )
        transactions_to_categorize_print["category"] = (
            transactions_to_categorize_print["category"]
            .apply(lambda str_: self.truncate_string_for_print(str_, 30))
        )
        print()
        print(transactions_to_categorize_print)

    @staticmethod
    def truncate_string_for_print(str_, width: int) -> str:
        if len(str_) > width:
            str_ = str_[: width - 3] + "..."
        return str_

    @staticmethod
    def print_dict_user_friendly(dict_):
        for key, value in dict_.items():
            print(f"{key}: {value}")

    def print_transaction_details(self, transaction):
        print("\n      ***** Transaction Details ******         \n")
        self.print_dict_user_friendly(transaction[
                [
                    "datetime",
                    "amount",
                    "account",
                    "third_party_category",
                    "pattern",
                    "category",
                ]
            ].to_dict())
        print()
        print(transaction["note"])
        if pd.notna(transaction["category"]):
            if pd.notna(transaction["pattern"]):
                print(
                    f"\nThis transaction is already categorized as "
                    f'**{transaction["category"]}** with pattern: '
                    f'**{transaction["pattern"]}**'
                )
            else:
                print(
                    f"\nThis transaction is already categorized as "
                    f'**{transaction["category"]}** '
                )

    def print_all_categories(self) -> None:
        all_categories_enum = []
        for i in range(len(self.all_categories)):
            all_categories_enum.append(f"{i}: {self.all_categories[i]}")
        cli = cmd.Cmd()
        print("\n      ***** All Categories ******         \n")
        cli.columnize(all_categories_enum, displaywidth=self.terminal_size.columns)

    def get_user_input_for_category(self) -> str:
        while True:
            try:
                inputted_category = input(
                    "\nCategorize this transaction by typing in category "
                    "or selecting index of pre-existing category (enter to skip, "
                    "enter '-' to clear the category):\n"
                )
                if inputted_category == "-":
                    inputted_category = None
                    break
                if inputted_category.isdigit():  # if integer
                    inputted_category = self.all_categories[int(inputted_category)]
                inputted_category = inputted_category.strip("/").lower()
                self.data_validation_engine.verify_category_format(inputted_category)
                break
            except KeyboardInterrupt:
                print("Exiting without saving")
                exit()
            except Exception as e:
                print(f"\n❌❌❌❌Invalid input. Please try again. {e}")
        return inputted_category

    def get_user_input_for_pattern(self, transaction, inputted_category) -> str:
        while True:
            try:
                inputted_pattern = input(
                    f"\nAdd a pattern for category **{inputted_category}** based "
                    f"on this transaction. Assume text is lower-cased. "
                    f"(enter to skip)\n\n{transaction['note']}\n\n"
                )
                if inputted_pattern == "":
                    break
                if inputted_pattern.isdigit():  # if integer
                    inputted_pattern = self.all_patterns[int(inputted_pattern)]
                self.data_validation_engine.verify_pattern_matches_text(
                    inputted_pattern, transaction["note"], hide_text=True
                )
                if (
                    inputted_pattern,
                    inputted_category,
                ) not in self.pattern_category_map_list:
                    # if new pattern -> cat mapping, validate
                    pattern_category_map_list_copy = deepcopy(
                        self.pattern_category_map_list
                    )
                    pattern_category_map_list_copy.append(
                        (inputted_pattern, inputted_category)
                    )
                    self.data_validation_engine.verify_no_pattern_maps_to_more_than_one_category(
                        pattern_category_map_list_copy
                    )
                break
            except KeyboardInterrupt:
                print("Exiting without saving")
                exit()
            except Exception as e:
                print(f"\n❌❌❌❌Invalid input. Please try again. Error: {e}")
        return inputted_pattern

    def save_categorized_transactions(self, categorized_transactions):
        """Write new history"""
        # reorder columns
        categorized_transactions = categorized_transactions[
            [
                "id",
                "datetime",
                "amount",
                "account",
                "third_party_category",
                "note",
                "pattern",
                "category",
            ]
        ]
        categorized_transactions.to_csv(
            self.historical_categorized_transactions_file_path, index=False
        )

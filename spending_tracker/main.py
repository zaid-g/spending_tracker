from dependency_injector.wiring import Provide, inject
from spending_tracker.engines.raw_data_processing_engine import RawDataProcessingEngine
from spending_tracker.engines.categorization_engine import CategorizationEngine
from spending_tracker.containers.categorization_container import CategorizationContainer
from copy import deepcopy
import datetime
import ipdb
import glob
import sys
import pandas as pd
import os
import json

pd.set_option("display.max_rows", 10000)


@inject
def main(
    raw_data_processing_engine: RawDataProcessingEngine = Provide[
        CategorizationContainer.raw_data_processing_engine
    ],
    categorization_engine: CategorizationEngine = Provide[
        CategorizationContainer.categorization_engine
    ],
) -> None:
    pass


if __name__ == "__main__":
    categorization_container = CategorizationContainer()
    categorization_container.config.root_data_folder_path.from_env(
        "SPENDING_TRACKER_DATA_PATH", required=True
    )
    categorization_container.config.from_yaml("./config.yml")
    categorization_container.wire(modules=[__name__])

    main()


# %% -------- [] ----------:









# store all possible categories and patterns in variable
(
    pattern_category_map_list,
    pattern_category_map_dict,
    all_categories,
    all_patterns,
) = extract_patterns_categories_from_history(hist_df)

# make sure no pattern maps to more than one category
make_sure_no_pattern_maps_to_more_than_one_category(pattern_category_map_list)


# assert all categories are valid in historical file
hist_df["category"].apply(lambda category: make_sure_category_dtype(category))

# make sure no ids are duplicated
assert len(hist_df) == len(
    hist_df.id.value_counts()
), "Error: found duplicate IDs in historical file"

# make sure all historical transactions are accounted for in cleaned_csvs
assert set(hist_df.id.values).issubset(
    df.id.values
), "Error: not all historical transactions accounted for in cleaned or raw csvs"

# apply patterns on new data
df["pattern"] = df["note"].apply(
    lambda text: get_matched_pattern(text, pattern_category_map_list)
)
df["category"] = df["pattern"].apply(
    lambda pattern: get_category_from_pattern(pattern, pattern_category_map_dict)
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

# ---------- [prompt user input and finish] ----------:


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
                make_sure_pattern_matches_text(
                    inputted_pattern, df.loc[transaction_index, "note"]
                )
                if (
                    inputted_pattern,
                    inputted_category,
                ) not in pattern_category_map_list:
                    pattern_category_map_list_copy = deepcopy(pattern_category_map_list)
                    pattern_category_map_list_copy.append(
                        (inputted_pattern, inputted_category)
                    )
                    make_sure_no_pattern_maps_to_more_than_one_category(
                        pattern_category_map_list_copy
                    )
                    pattern_category_map_list.append(
                        (inputted_pattern, inputted_category)
                    )
                    pattern_category_map_dict[inputted_pattern] = inputted_category
                    df.loc[transaction_index, "pattern"] = inputted_pattern
                    if inputted_category not in all_categories:
                        all_categories.append(inputted_category)
                    if inputted_pattern not in all_patterns:
                        all_patterns.append(inputted_pattern)
                break
            except:
                print("Error: inputted pattern does not match text (note)")

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

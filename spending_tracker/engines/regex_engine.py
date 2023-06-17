import pandas as pd
import numpy as np
import re




def make_sure_no_pattern_maps_to_more_than_one_category(pattern_category_map_list):
    patterns = sorted(list(set([pattern for pattern, _ in pattern_category_map_list])))
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


def make_sure_category_dtype(category):
    if pd.isna(category):
        return
    assert type(category) == str, "category must be string"


def make_sure_pattern_matches_text(pattern, text):
    text = text.lower()
    if pd.isna(pattern):
        return
    assert (
        re.compile(pattern).search(text) != None
    ), f"Error: found pattern that doesn't match note (text). Pattern: {pattern} --- Text: {text}"


def extract_patterns_categories_from_history(hist_df):
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


def get_matched_pattern(text, pattern_category_map_list):
    """Returns longest pattern that matches text"""
    matched_patterns = []
    for pattern, _ in pattern_category_map_list:
        if re.compile(pattern).search(text.lower()) != None:
            matched_patterns.append(pattern)
    if len(matched_patterns) == 0:
        return
    return max(matched_patterns, key=len)


def get_category_from_pattern(pattern, pattern_category_map_dict):
    if pattern == None:
        return None
    else:
        return pattern_category_map_dict[pattern]

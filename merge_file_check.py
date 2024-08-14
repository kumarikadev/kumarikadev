import pandas as pd
import re
from geopy.geocoders import Nominatim
from geotext import GeoText

from constants import COLUMN_DICTIONARY
from data_read import read_latest_data

geolocator = Nominatim(user_agent="geoapiExercises")


def calculate_consistency(df, col_1, col_2, table1, table2):
    """Function to calculate consistency"""
    original_df = df.copy(deep=True)
    accuracy_col = f"Accurate_{col_1}"
    accurate_count, total_count = None, None
    if (df.dtypes[col_1] == "object") & (df.dtypes[col_2] == "object"):
        if col_1 == f"Regulated Status_{table1}":
            if table2 == "INTACT":
                df[accuracy_col] = (df[col_1] == "Dual Regulated") & (df[f"Dual Regulated_{table2}"] == 1)
            else:
                df[accuracy_col] = (df[col_1] == "Dual Regulated") & (df[col_2] == "Dual Regulated")
        else:
            df[accuracy_col] = df[col_1].str.lower() == df[col_2].str.lower()
        accurate_count = df[accuracy_col].sum()
        total_count = len(df[col_1])
        return accurate_count, total_count, original_df[~df[accuracy_col]]
    return accurate_count, total_count, pd.DataFrame()


def calculate_completeness(df, col_1, col_2):
    """Function to calculate completeness"""
    original_df = df.copy(deep=True)
    completeness_col = f"Complete_{col_1}"
    df[completeness_col] = df[col_1].isna() | df[col_2].isna()
    complete_count = df[completeness_col].sum()
    total_count = len(df[completeness_col])
    return (
        complete_count,
        total_count,
        original_df[~df[completeness_col]],
    )


def calculate_validity(df, col_1, col_2, pattern):
    """Function to calculate validity (example: checking LEI format)"""
    original_df = df.copy(deep=True)
    validity_col = f"Valid_{col_1}"
    regex = re.compile(pattern)
    df[validity_col] = df[col_1].apply(
        lambda x: bool(regex.match(str(x))) if pd.notnull(x) else False
    ) & df[col_2].apply(lambda x: bool(regex.match(str(x))) if pd.notnull(x) else False)
    valid_count = df[validity_col].sum()
    total_count = len(df[validity_col])
    return valid_count, total_count, original_df[~df[validity_col]]


def get_country(location):
    """Function to get country from location"""
    try:
        loc = geolocator.geocode(location, language="en", addressdetails=True)
        if loc and "country" in loc.raw["address"]:
            return loc.raw["address"]["country"]
    except Exception as e:
        return None


def is_sub_part(row, col_1, col_2):
    """Function to check if column2 is part of column1"""
    if row[col_1] == row[col_2]:
        return True
    country1 = get_country(row[col_1])
    country2 = get_country(row[col_2])
    if country1 == country2:
        return True
    return False


def main(df, column_pair, table1, table2):
    merge_df = df.copy(deep=True)
    comparison_name = f"{table1} VS {table2}"
    result_list = []
    lei_pattern = r"^[A-Z0-9]{20}$"  # LEI pattern: length=20 and alphanumeric

    consistency_error_df = pd.DataFrame()
    completeness_error_df = pd.DataFrame()
    country_error_df = pd.DataFrame()

    # Perform data quality checks for each column pair
    for table1_col, table2_col in column_pair:
        if table1_col is None or table2_col is None:
            continue

        table1_col = table1_col + f"_{table1}"
        table2_col = table2_col + f"_{table2}"
    
        consistency_accurate_count, consistency_total_count, consistency_error_df = (
            calculate_consistency(merge_df, table1_col, table2_col, table1, table2)
        )
        completeness_accurate_count, completeness_total_count, completeness_error_df = (
            calculate_completeness(merge_df, table1_col, table2_col)
        )

        # Check if columns are LEI for validity check
        if "LEI" in table1_col or "LEI" in table2_col:
            valid_count, valid_total_count, validity_error_df = calculate_validity(
                merge_df, table1_col, table2_col, lei_pattern
            )
        else:
            valid_count, valid_total_count = None, None

        # country check
        country_correctness = None
        country_count = None
        if table1_col in [
            f"Country of Ownership_{table1}",
            f"Country of Incorporation_{table1}",
        ] or table2_col in [
            f"Country of Ownership_{table2}",
            f"Country of Incorporation_{table2}",
        ]:
            merge_df["is_sub_part"] = merge_df.apply(
                lambda row: is_sub_part(row, table1_col, table2_col), axis=1
            )
            country_correctness = merge_df["is_sub_part"].sum()
            country_count = len(merge_df)
            country_error_df = df[~merge_df["is_sub_part"]]

        consistency_error_df["table1_column"] = completeness_error_df["table1_column"] = country_error_df["table1_column"] = table1_col
        consistency_error_df["table2_column"] = completeness_error_df["table2_column"] = country_error_df["table2_column"] = table2_col

        result_list.append(
            [
                comparison_name,
                table1_col,
                table2_col,
                consistency_accurate_count,
                consistency_total_count,
                completeness_accurate_count,
                completeness_total_count,
                valid_count,
                valid_total_count,
                country_correctness,
                country_count,
            ]
        )

    # Create a DataFrame for the data quality results
    dq_columns = [
        "comparison_name",
        "table1_column",
        "table2_column",
        "consistency_accurate_count",
        "consistency_total_count",
        "completeness_accurate_count",
        "completeness_total_count",
        "valid_count",
        "valid_total_count",
        "country_correctness",
        "country_count",
    ]
    data_quality_df = pd.DataFrame(result_list, columns=dq_columns)

    error_df = pd.concat(
        [
            consistency_error_df,
            completeness_error_df,
            country_error_df,
            validity_error_df,
        ],
        ignore_index=True,
    )
    error_df["comparison_name"] = comparison_name

    return data_quality_df, error_df


if __name__ == "__main__":

    rwm_df, bsmr_df, intact_df = read_latest_data()
    RWM = rwm_df.add_suffix("_RWM")
    BSMR = bsmr_df.add_suffix("_BSMR")
    INTACT = intact_df.add_suffix("_INTACT")

    # Merge RWM_INTACT
    merged_RWM_INTACT = pd.merge(
        RWM, INTACT, left_on="FRN_RWM", right_on="FRN_INTACT", how="inner"
    )
    RWM_INTACT_processed, RWM_INTACT_error = main(
        merged_RWM_INTACT,
        zip(COLUMN_DICTIONARY["RWM"], COLUMN_DICTIONARY["INTACT"]),
        "RWM",
        "INTACT",
    )

    # Merge BSMR_INTACT
    merged_BSMR_INTACT = pd.merge(
        BSMR, INTACT, left_on="FRN_BSMR", right_on="FRN_INTACT", how="inner"
    )
    BSMR_INTACT_processed, BSMR_INTACT_error = main(
        merged_BSMR_INTACT,
        zip(COLUMN_DICTIONARY["BSMR"], COLUMN_DICTIONARY["INTACT"]),
        "BSMR",
        "INTACT",
    )

    # Merge RWM_BSMR
    merged_RWM_BSMR = pd.merge(
        RWM, BSMR, left_on="FRN_RWM", right_on="FRN_BSMR", how="inner"
    )
    RWM_BSMR_processed, RWM_BSMR_error = main(
        merged_RWM_BSMR,
        zip(COLUMN_DICTIONARY["RWM"], COLUMN_DICTIONARY["BSMR"]),
        "RWM",
        "BSMR",
    )

    summary_df = pd.concat(
        [RWM_INTACT_processed, BSMR_INTACT_processed, RWM_BSMR_processed],
        ignore_index=True,
    )
    error_df = pd.concat(
        [RWM_INTACT_error, BSMR_INTACT_error, RWM_BSMR_error], ignore_index=True
    )

    merge_summ_err_df = pd.merge(
        summary_df,
        error_df,
        left_on=["comparison_name", "table1_column", "table2_column"],
        right_on=["comparison_name", "table1_column", "table2_column"],
        how="left",
    )

    merge_summ_err_df.to_csv("output\\merge_df.csv")

    
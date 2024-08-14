import pandas as pd


RWM_DATA_PATH = "input_data\\rwm.csv"
BSMR_DATA_PATH = "input_data\\bsmr.csv"
INTACT_DATA_PATH = "input_data\\intact.csv"

columns_to_drop = [
    "Unnamed: 0",
    "Regulated Activity: Regulated Activity Name",
    "Country of Ownership",
]

def read_latest_data() -> pd.DataFrame:
    """Read and return data in dataframe format"""
    rwm_df = pd.read_csv(RWM_DATA_PATH)
    rwm_df = rwm_df.drop(columns_to_drop, axis=1, errors="ignore")

    bsmr_df = pd.read_csv(BSMR_DATA_PATH)
    bsmr_df = bsmr_df.drop(columns_to_drop, axis=1, errors="ignore")
    
    intact_df = pd.read_csv(INTACT_DATA_PATH)
    intact_df = intact_df.drop(columns_to_drop, axis=1, errors="ignore")

    return rwm_df, bsmr_df, intact_df

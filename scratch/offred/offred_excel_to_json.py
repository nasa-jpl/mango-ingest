from typing import List
import pandas as pd
import json

def excel_2_json(excel_fpath: str, json_fpath: str, columns_to_keep: List[str]):
    """
    Create JSON file with metadata for OFFRED variables from Excel table
    Args:
        excel_filepath (str): Path to the Excel file that describes OFFRED data
        json_fpath: (str): Path to output file in JSON format
        columns_to_keep: (list): List of column names in Excel table to include in the JSON file
    """

    # Read the Excel file.
    df = pd.read_excel(excel_fpath, sheet_name='Sheet1', header=1, dtype=str)
    columns_to_drop = list(set(df.columns) - set(columns_to_keep))

    # Drop columns that we don't need
    df = df.drop(columns=columns_to_drop, axis=1)

    # convert to dictionary
    result_dict = df.set_index('NAME').to_dict('index')

    with open(json_fpath, "w") as json_file:
        json.dump(result_dict, json_file, indent=4)


if __name__ == '__main__':
    columns_to_keep = ["NAME", "DESCR", "UNIT"]
    excel_fpath = '<Excel file path>'
    json_fpath = '<output file path>'
    excel_2_json(excel_fpath, json_fpath, columns_to_keep)
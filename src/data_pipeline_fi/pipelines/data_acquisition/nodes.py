"""
nodes.py

These are the nodes from pipeline 'data_acquisition'.

Author:
    Vinícius Peres (viniaperes@gmail.com)

Version:
    1.0.0

Release Date:
    May 9th, 2023

"""

import io
import re
import zipfile
from typing import Union, List, Tuple

import requests
import yaml


def generate_urls(months: Union[str, List[str]]) -> Tuple[str]:
    """Take the month(s) defined in parameters config file (data_acquisition.yml) as input and generates the list of
    urls to download the .zip files from https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/.

    Args:
        months: Months in which .zip files must be downloaded.
    Returns:
        List of urls to download the .zip files.
    """

    # Check if YAML interpreted the input as number or string
    if not isinstance(months, str):
        months = str(months)

    # Remove any whitespaces that might exist and split the string
    months_clean = months.replace(" ", "")
    months_clean = months_clean.split(",")

    # Convert string list into integer list
    months_int = [int(x) for x in months_clean]

    # Generate all months if 13 was chosen
    if 13 in months_int:
        months_int = list(range(1, 13))

    # Generate the zip files urls
    url_base = r"https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/"
    url_list = []
    for month in months_int:
        if month > 9:
            url = url_base + f"cda_fi_2022{month}.zip"
        else:
            url = url_base + f"cda_fi_20220{month}.zip"
        url_list.append(url)

    return tuple(url_list)


def download_files(url_list: Tuple[str]) -> Tuple[str]:
    """Take the list of urls to download the .zip files as input and extract the .csv files to the data/01_raw folder.

    Args:
        url_list: List of urls to download the .zip files.
    Returns:
        List of .csv filenames.
    """

    # Regex to extract only the desired .csv files
    regex = re.compile(r"cda_fi_BLC_[0-9]_2022[0-1][0-9].csv")

    # For each url (month), download the .zip file and extract all the desired .csv files to the data/01_raw folder
    files_list = []
    for url in url_list:
        # Download the .zip file
        response = requests.get(url)
        if response.ok:
            # Read the .zip file
            z = zipfile.ZipFile(io.BytesIO(response.content))
            # Get the filenames inside the .zip file
            filenames = z.namelist()
            # Filter the filenames according to the previously defined regex
            filenames = list(filter(regex.search, filenames))
            files_list = files_list + filenames
            # Extract the desired .csv files to the data/01_raw folder
            z.extractall("data/01_raw", filenames)

    # Custom catalog to add the downloaded datasets to the catalog
    catalog_custom = dict()
    # Dictionary to save the .csv filenames as parameters in data_processing.yml
    parameters = dict()
    # Fill in both dictionaries
    filenames_list = []
    for file in files_list:
        filename = f"{file.replace('.csv', '')}"
        filenames_list.append(filename)
        parameters[filename] = filename
        catalog_custom[filename] = {
            "type": "pandas.CSVDataSet",
            "load_args": {
                "encoding": "cp1252",
                "sep": ";"
            },
            "save_args": {
                "encoding": "utf-8",
                "sep": ";"
            },
            "filepath": f"data/01_raw/{file}"
        }

    # Save the custom catalog
    with open("conf/base/catalog_custom.yml", "w") as file:
        yaml.dump(catalog_custom, file)

    # Save the filenames in the data_processing config parameters file
    with open("conf/base/parameters/data_processing.yml", "w") as file:
        yaml.dump(parameters, file)

    # Return the list of .csv filenames
    return tuple(filenames_list)

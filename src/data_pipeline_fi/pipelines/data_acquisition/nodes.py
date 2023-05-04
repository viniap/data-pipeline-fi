"""
This is a boilerplate pipeline 'data_acquisition'
generated using Kedro 0.18.7
"""

from typing import Union, List, Tuple
import requests, zipfile, io, re


def generate_urls(months: Union[str, List[str]]) -> Tuple[str]:
    # Check if YAML interpreted the input as number or string
    if not isinstance(months, str):
        months = str(months)

    # Remove any whitespaces that might exist and split the string
    months_clean = months.replace(" ", "")
    months_clean = months_clean.split(",")

    # Convert string list into integer list
    months_int = [int(x) for x in months_clean]

    # Generate all months if 13 was writen
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

    # print(url_list)
    return tuple(url_list)


def download_files(url_list: Tuple[str]) -> Tuple[str]:
    regex = re.compile(r"cda_fi_BLC_[0-9]_2022[0-1][0-9].csv")
    filenames_list = []
    for url in url_list:
        response = requests.get(url)
        if response.ok:
            z = zipfile.ZipFile(io.BytesIO(response.content))
            filenames = z.namelist()
            filenames = list(filter(regex.search, filenames))
            # print(filenames)
            filenames_list = filenames_list + filenames
            z.extractall("data/01_raw", filenames)

    return tuple(filenames_list)

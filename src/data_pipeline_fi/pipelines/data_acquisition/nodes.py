"""
This is a boilerplate pipeline 'data_acquisition'
generated using Kedro 0.18.7
"""

from typing import Union, List, Tuple
import requests, zipfile, io, re, yaml


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
    files_list = []
    for url in url_list:
        response = requests.get(url)
        if response.ok:
            z = zipfile.ZipFile(io.BytesIO(response.content))
            filenames = z.namelist()
            filenames = list(filter(regex.search, filenames))
            # print(filenames)
            files_list = files_list + filenames
            z.extractall("data/01_raw", filenames)

    catalog_custom = dict()
    parameters = dict()
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

    with open("conf/base/catalog_custom.yml", "w") as file:
        yaml.dump(catalog_custom, file)

    with open("conf/base/parameters/data_processing.yml", "w") as file:
        yaml.dump(parameters, file)

    return tuple(filenames_list)

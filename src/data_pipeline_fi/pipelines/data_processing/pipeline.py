"""
pipeline.py

This is the pipeline 'data_processing', which is responsible for processing the datasets.

Author:
    Vinícius Peres (viniaperes@gmail.com)

Version:
    1.0.0

Release Date:
    May 9th, 2023

"""

import yaml
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import merge_dataframes, sum_vl_mercado, encode_tp_ativo, export_to_postgresql


def create_pipeline(**kwargs) -> Pipeline:
    # Get the data_processing parameters
    with open("conf/base/parameters/data_processing.yml") as file:
        data_processing_parameters = yaml.load(file, Loader=yaml.FullLoader)
    return pipeline([
        node(
            func=merge_dataframes,
            inputs=[key for key, value in data_processing_parameters.items()] + ["filenames"],
            outputs="cda_fi_BLC_all",
            name="merge_dataframes_node",
        ),
        node(
            func=sum_vl_mercado,
            inputs="cda_fi_BLC_all",
            outputs="vl_mercado",
            name="sum_vl_mercado_node",
        ),
        node(
            func=encode_tp_ativo,
            inputs="cda_fi_BLC_all",
            outputs="diversificacoes",
            name="encode_tp_ativo_node",
        ),
        node(
            func=export_to_postgresql,
            inputs=["vl_mercado", "diversificacoes"],
            outputs=["vl_mercado_table_dataset", "diversificacoes_table_dataset"],
            name="export_to_postgresql_node",
        ),
    ])

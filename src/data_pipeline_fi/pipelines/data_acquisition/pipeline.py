"""
pipeline.py

This is the pipeline 'data_acquisition', which is responsible for getting the datasets.

Author:
    Vinícius Peres (viniaperes@gmail.com)

Version:
    1.0.0

Release Date:
    May 9th, 2023

"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import generate_urls, download_files


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=generate_urls,
            inputs="params:months",
            outputs="url_list",
            name="generate_urls_node"
        ),
        node(
            func=download_files,
            inputs="url_list",
            outputs="filenames",
            name="download_files_node"
        ),
    ])

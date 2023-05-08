"""
This is a boilerplate pipeline 'data_acquisition'
generated using Kedro 0.18.7
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

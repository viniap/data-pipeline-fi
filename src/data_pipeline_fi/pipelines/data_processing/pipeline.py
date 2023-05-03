"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.7
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import merge_dataframes, aggregate


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=merge_dataframes,
            inputs=[f"cda_fi_BLC_{i}_202212" for i in range(1, 9)],
            outputs="cda_fi_BLC_all_202212",
            name="merge_dataframes_node",
        ),
        node(
            func=aggregate,
            inputs="cda_fi_BLC_all_202212",
            outputs="cda_fi_BLC_all_202212_aggregated",
            name="aggregate_node",
        ),
    ])

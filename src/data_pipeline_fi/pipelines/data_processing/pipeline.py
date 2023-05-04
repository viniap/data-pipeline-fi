"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.7
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import merge_dataframes, sum_vl_mercado, encode_tp_ativo, export_to_postgresql


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=merge_dataframes,
            inputs=[f"cda_fi_BLC_{i}_202212" for i in range(1, 9)],
            outputs="cda_fi_BLC_all_202212",
            name="merge_dataframes_node",
        ),
        node(
            func=sum_vl_mercado,
            inputs="cda_fi_BLC_all_202212",
            outputs="vl_mercado",
            name="sum_vl_mercado_node",
        ),
        node(
            func=encode_tp_ativo,
            inputs="cda_fi_BLC_all_202212",
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

from typing import Optional, Union

from compose import compose

from db import bigquery
from tasks import cloud_tasks
from shopify import pipeline, shop, shopify_repo


def pipeline_service(
    pipeline: pipeline.interface.Pipeline,
    shop: shop.interface.Shop,
    start: Optional[str],
    end: Optional[str],
) -> dict[str, Union[str, int]]:
    table = f"{shop.name}__{pipeline.table}"
    
    return compose(
        lambda x: {
            "table": pipeline.table,
            "shop_url": shop.shop_url,
            "start": start,
            "end": end,
            "output_rows": x,
        },
        bigquery.load(
            table,
            pipeline.schema,
            pipeline.id_key,
            pipeline.cursor_key,
        ),
        pipeline.transform,
        shopify_repo.get(pipeline.resource, shop),
        bigquery.get_last_timestamp(table, pipeline.cursor_key),
    )((start, end))


def tasks_service(body: dict[str, str]):
    return {
        "tasks": cloud_tasks.create_tasks(
            [
                {
                    "resource": p,
                    "shop": s,
                    "start": body.get("start"),
                    "end": body.get("end"),
                }
                for p in pipeline.pipelines.keys()
                for s in shop.shops.keys()
            ],
            lambda x: f"{x['resource']}-{x['shop']}",
        )
    }

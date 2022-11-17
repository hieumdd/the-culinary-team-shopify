from shopify.pipeline import orders, draft_orders


pipelines = {
    i.table: i
    for i in [
        j.pipeline  # type: ignore
        for j in [
            orders,
            # draft_orders,
        ]
    ]
}

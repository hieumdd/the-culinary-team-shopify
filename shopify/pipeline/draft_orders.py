from shopify.pipeline.interface import Pipeline, Resource

pipeline = Pipeline(
    "DraftOrders",
    Resource(
        "draft_orders.json",
        "draft_orders",
        [
            "id",
            "completed_at",
            "created_at",
            "updated_at",
            "currency",
            "name",
            "email",
            "customer",
            "subtotal_price",
            "total_price",
            "total_tax",
            "line_items",
            "tags",
        ],
    ),
    lambda rows: [
        {
            "id": row.get("id"),
            "completed_at": row.get("completed_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "currency": row.get("currency"),
            "name": row.get("name"),
            "email": row.get("email"),
            "customer": {
                "id": row["customer"].get("id"),
                "email": row["customer"].get("email"),
                "first_name": row["customer"].get("first_name"),
                "last_name": row["customer"].get("last_name"),
                "phone": row["customer"].get("phone"),
            }
            if row.get("customer")
            else {},
            "subtotal_price": row.get("subtotal_price"),
            "total_price": row.get("total_price"),
            "total_tax": row.get("total_tax"),
            "line_items": [
                {
                    "id": line_item.get("id"),
                    "product_id": line_item.get("product_id"),
                    "sku": line_item.get("sku"),
                    "title": line_item.get("title"),
                    "quantity": line_item.get("quantity"),
                    "price_set": {
                        "amount": line_item["price_set"].get("amount"),
                        "currency_code": line_item["price_set"].get("currency_code"),
                    }
                    if line_item.get("price_set")
                    else {},
                }
                for line_item in row["line_items"]
            ]
            if row.get("line_items")
            else [],
            "tags": row.get("tags"),
        }
        for row in rows
    ],
    [
        {"name": "id", "type": "NUMERIC"},
        {"name": "completed_at", "type": "TIMESTAMP"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "currency", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {
            "name": "customer",
            "type": "RECORD",
            "fields": [
                {"name": "id", "type": "NUMERIC"},
                {"name": "email", "type": "STRING"},
                {"name": "first_name", "type": "STRING"},
                {"name": "last_name", "type": "STRING"},
                {"name": "phone", "type": "STRING"},
            ],
        },
        {"name": "subtotal_price", "type": "NUMERIC"},
        {"name": "total_price", "type": "NUMERIC"},
        {"name": "total_tax", "type": "NUMERIC"},
        {
            "name": "line_items",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "id", "type": "NUMERIC"},
                {"name": "product_id", "type": "NUMERIC"},
                {"name": "sku", "type": "STRING"},
                {"name": "title", "type": "STRING"},
                {"name": "quantity", "type": "NUMERIC"},
                {
                    "name": "price_set",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {"name": "amount", "type": "NUMERIC"},
                        {"name": "currency_code", "type": "STRING"},
                    ],
                },
            ],
        },
        {"name": "tags", "type": "STRING"},
    ],
)

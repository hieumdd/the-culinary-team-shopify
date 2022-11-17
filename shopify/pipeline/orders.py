from shopify.pipeline.interface import Pipeline, Resource

pipeline = Pipeline(
    "Orders",
    Resource(
        "orders.json",
        "orders",
        [
            "id",
            "closed_at",
            "created_at",
            "updated_at",
            "currency",
            "name",
            "order_number",
            "email",
            "customer",
            "referring_site",
            "fulfillment_status",
            "current_total_discounts",
            "current_total_price",
            "current_subtotal_price",
            "current_total_tax",
            "subtotal_price",
            "total_discounts",
            "total_price",
            "total_tax",
            "refunds",
            "total_shipping_price_set",
            "line_items",
            "tags",
        ],
        {"status": "any"},
    ),
    lambda rows: [
        {
            "id": row.get("id"),
            "closed_at": row.get("closed_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "name": row.get("name"),
            "order_number": row.get("order_number"),
            "email": row.get("email"),
            "currency": row.get("currency"),
            "customer": {
                "id": row["customer"].get("id"),
                "email": row["customer"].get("email"),
                "first_name": row["customer"].get("first_name"),
                "last_name": row["customer"].get("last_name"),
                "phone": row["customer"].get("phone"),
            }
            if row.get("customer")
            else {},
            "referring_site": row.get(
                "referring_site",
            ),
            "fulfillment_status": row.get("fulfillment_status"),
            "current_total_discounts": row.get("current_total_discounts"),
            "current_total_price": row.get("current_total_price"),
            "current_subtotal_price": row.get("current_subtotal_price"),
            "current_total_tax": row.get("current_total_tax"),
            "subtotal_price": row.get("subtotal_price"),
            "total_discounts": row.get("total_discounts"),
            "total_price": row.get("total_price"),
            "total_tax": row.get("total_tax"),
            "refunds": [
                {
                    "id": refund.get("id"),
                    "created_at": refund.get("created_at"),
                    "transactions": [
                        {
                            "id": transaction.get("id"),
                            "amount": transaction.get("amount"),
                            "created_at": transaction.get("created_at"),
                            "processed_at": transaction.get("processed_at"),
                            "amount": transaction.get("amount"),
                        }
                        for transaction in refund["transactions"]
                    ]
                    if refund.get("transactions")
                    else [],
                }
                for refund in row["refunds"]
            ]
            if row.get("refunds")
            else [],
            "total_shipping_price_set": {
                "shop_money": {
                    "amount": row["total_shipping_price_set"]["shop_money"].get(
                        "amount"
                    ),
                    "currency_code": row["total_shipping_price_set"]["shop_money"].get(
                        "currency_code"
                    ),
                }
                if row["total_shipping_price_set"].get("shop_money")
                else {},
                "presentment_money": {
                    "amount": row["total_shipping_price_set"]["presentment_money"].get(
                        "amount"
                    ),
                    "currency_code": row["total_shipping_price_set"][
                        "presentment_money"
                    ].get("currency_code"),
                }
                if row["total_shipping_price_set"].get("presentment_money")
                else {},
            }
            if row.get("total_shipping_price_set")
            else {},
            "line_items": [
                {
                    "id": line_item.get("id"),
                    "product_id": line_item.get("product_id"),
                    "sku": line_item.get("sku"),
                    "title": line_item.get("title"),
                    "quantity": line_item.get("quantity"),
                    "price": line_item.get("price"),
                    "total_discount": line_item.get("total_discount"),
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
        {"name": "closed_at", "type": "TIMESTAMP"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "currency", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "order_number", "type": "NUMERIC"},
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
        {"name": "referring_site", "type": "STRING"},
        {"name": "fulfillment_status", "type": "STRING"},
        {"name": "current_total_discounts", "type": "NUMERIC"},
        {"name": "current_total_price", "type": "NUMERIC"},
        {"name": "current_subtotal_price", "type": "NUMERIC"},
        {"name": "current_total_tax", "type": "NUMERIC"},
        {"name": "subtotal_price", "type": "NUMERIC"},
        {"name": "total_discounts", "type": "NUMERIC"},
        {"name": "total_price", "type": "NUMERIC"},
        {"name": "total_tax", "type": "NUMERIC"},
        {
            "name": "refunds",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "id", "type": "NUMERIC"},
                {"name": "created_at", "type": "TIMESTAMP"},
                {
                    "name": "transactions",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {"name": "id", "type": "NUMERIC"},
                        {"name": "amount", "type": "NUMERIC"},
                        {"name": "status", "type": "STRING"},
                        {"name": "created_at", "type": "TIMESTAMP"},
                        {"name": "processed_at", "type": "TIMESTAMP"},
                    ],
                },
                {"name": "last_name", "type": "STRING"},
                {"name": "phone", "type": "STRING"},
            ],
        },
        {
            "name": "total_shipping_price_set",
            "type": "RECORD",
            "fields": [
                {
                    "name": "shop_money",
                    "type": "RECORD",
                    "fields": [
                        {"name": "amount", "type": "NUMERIC"},
                        {"name": "currency_code", "type": "STRING"},
                    ],
                },
                {
                    "name": "presentment_money",
                    "type": "RECORD",
                    "fields": [
                        {"name": "amount", "type": "NUMERIC"},
                        {"name": "currency_code", "type": "STRING"},
                    ],
                },
            ],
        },
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
                {"name": "price", "type": "NUMERIC"},
                {"name": "total_discount", "type": "NUMERIC"},
            ],
        },
        {"name": "tags", "type": "STRING"},
    ],
)

import os

from shopify.shop import interface


shops = {
    i.shop_url: i
    for i in [
        interface.Shop(
            "NationalMemo",
            "national-memo",
            os.getenv("NATIONAL_MEMO_TOKEN", ""),
        ),
    ]
}

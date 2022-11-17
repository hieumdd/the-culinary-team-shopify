import pytest

from shopify import pipeline, shop, shopify_service

TIMEFRAME = [
    ("auto", (None, None)),
    # ("manual", ("2020-01-01", "2022-11-17")),
]


@pytest.fixture(
    params=[i[1] for i in TIMEFRAME],
    ids=[i[0] for i in TIMEFRAME],
)
def timeframe(request):
    return request.param


@pytest.fixture(  # type: ignore
    params=pipeline.pipelines.values(),
    ids=pipeline.pipelines.keys(),
)
def pipeline_(request):
    return request.param


@pytest.fixture(  # type: ignore
    params=shop.shops.values(),
    ids=shop.shops.keys(),
)
def shop_(request):
    return request.param


def test_shopify_service(pipeline_, shop_, timeframe):
    res = shopify_service.pipeline_service(
        pipeline_,
        shop_,
        timeframe[0],
        timeframe[1],
    )
    res


def test_service(timeframe):
    res = shopify_service.tasks_service(
        {
            "start": timeframe[0],
            "end": timeframe[1],
        }
    )
    res

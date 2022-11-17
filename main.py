from shopify import pipeline, shop, shopify_service


def main(request):
    data = request.get_json()
    print(data)

    if "shop" in data:
        response = shopify_service.pipeline_service(
            pipeline.pipelines[data["resource"]],
            shop.shops[data["shop"]],
            data.get("start"),
            data.get("end"),
        )
    else:
        response = shopify_service.tasks_service(data)

    print(response)
    return response

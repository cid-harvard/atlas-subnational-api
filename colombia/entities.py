from . import models

metadata_apis = {
    "product": {
        "entity_model": models.HSProduct,
        "plural": "products",
    },
    "location": {
        "entity_model": models.Location,
        "plural": "locations",
    },
}


{
    "product": {
        (("location", "department"), ("year", None)): {
            "name": "department_product_year",
            "action": lambda x: "cats"
        },
    },
    "location": {
        (("year", None)): {
            "name": "department_year",
            "action": lambda x: "dogs"
        }
    }
}

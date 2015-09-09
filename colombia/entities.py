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
    "industry": {
        "entity_model": models.Industry,
        "plural": "industries",
    },
    "country": {
        "entity_model": models.Country,
        "plural": "countries",
    },
    "occupation": {
        "entity_model": models.Occupation,
        "plural": "occupations",
    },
}

entities = {
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
    },
    "year":{
    }
}

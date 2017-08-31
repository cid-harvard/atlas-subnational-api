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
    "livestock": {
        "entity_model": models.Livestock,
        "plural": "livestock",
    },
    "agproduct": {
        "entity_model": models.AgriculturalProduct,
        "plural": "agproducts",
    },
    "nonag": {
        "entity_model": models.NonagriculturalActivity,
        "plural": "nonags",
    },
    "land_use": {
        "entity_model": models.LandUse,
        "plural": "land_uses",
    },
    "farmtype": {
        "entity_model": models.FarmType,
        "plural": "farmtypes",
    },
    "farmsize": {
        "entity_model": models.FarmSize,
        "plural": "farmsizes",
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

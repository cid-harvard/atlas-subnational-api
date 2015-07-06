import pandas as pd

from linnaeus import classification

product_classification = classification.load("product/HS/Atlas/out/hs92_atlas.csv")
location_classification = classification.load("location/Colombia/DANE/out/locations_colombia_dane.csv")
industry_classification = classification.load("industry/ISIC/Colombia/out/isic_ac_3.0.csv")


def first(x):
    """Return first element of a group in a pandas GroupBy object"""
    return x.nth(0)



trade4digit_department = {
    "read_function": lambda: pd.read_stata("/Users/makmana/ciddata/Aduanas/exp_ecomplexity_dpto_oldstata.dta"),
    "field_mapping": {
        "r": "department",
        "p": "product",
        "yr": "year",
        "X_rpy_p": "export_value",
        "density_natl": "density",
        "eci_natl": "eci",
        "pci": "pci",
        "coi_natl": "coi",
        "cog_natl": "cog",
        "RCA_natl": "export_rca"
    },
    "classification_fields": {
        "department": {
            "classification": location_classification,
            "level": "department"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "department": 2,
        "product": 4
    },
    "facet_fields": ["department", "product", "year"],
    "facets": {
        ("department_id", "year"): {
            "eci": first,
            "export_value": lambda x: x.sum()
        },
        ("product_id", "year"): {
            "pci": first,
            "export_value": lambda x: x.sum()
        },
        ("department_id", "product_id", "year"): {
            "export_value": first,
            "export_rca": first,
            "density": first,
            "cog": first,
            "coi": first,
            "eci": first
        }
    }
}

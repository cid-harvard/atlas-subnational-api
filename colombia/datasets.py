import pandas as pd
import os.path

from linnaeus import classification

product_classification = classification.load("product/HS/Atlas/out/hs92_atlas.csv")
location_classification = classification.load("location/Colombia/DANE/out/locations_colombia_dane.csv")
industry_classification = classification.load("industry/ISIC/Colombia/out/isic_ac_3.0.csv")


def first(x):
    """Return first element of a group in a pandas GroupBy object"""
    return x.nth(0)


DATASET_ROOT = "/Users/makmana/ciddata/Subnationals/"


def prefix_path(to_prefix):
    return os.path.join(DATASET_ROOT, to_prefix)


trade4digit_department = {
    "read_function": lambda: pd.read_stata(prefix_path("Atlas/Colombia/beta/Trade/exp_ecomplexity_r2.dta")),
    "field_mapping": {
        "r": "department",
        "p4": "product",
        "yr": "year",
        "X_rpy_d": "export_value",
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
        },
        ("product_id", "year"): {
            "pci": first,
        },
        ("department_id", "product_id", "year"): {
            "export_value": first,
            "export_rca": first,
            "density": first,
            "cog": first,
            "coi": first
        }
    }
}


trade4digit_municipality = {
    "read_function": lambda: pd.read_stata(prefix_path("atlas/colombia/beta/trade/exp_rpy_r5_p4.dta")),
    "field_mapping": {
        "r": "municipality",
        "p": "product",
        "yr": "year",
        "X_rpy_d": "export_value",
        "NP_rpy": "num_plants"
    },
    "classification_fields": {
        "municipality": {
            "classification": location_classification,
            "level": "municipality"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "municipality": 5,
        "product": 4
    },
    "facet_fields": ["municipality", "product", "year"],
    "facets": {
        ("municipality_id", "product_id", "year"): {
            "export_value": first,
            "num_plants": first
        }
    }
}


pila_to_atlas = {
    "r": "department",
    "i": "industry",
    "year": "year",
    "E_yir": "employment",
    "W_yir": "wages",
    "rca": "rca",
    "density": "density",
    "cog": "cog",
    "coi": "coi",
    "pci": "complexity"
}

pila_to_atlas_muni = dict(pila_to_atlas.items())
pila_to_atlas_muni["r"] = "municipality"

industry4digit_department = {
    "read_function": lambda: pd.read_stata(prefix_path("Atlas/Colombia/beta/PILA_andres/COL_PILA_ecomp-E_yir_2008-2012_rev3_dpto.dta")),
    "field_mapping": pila_to_atlas,
    "hook_pre_merge": lambda df: df[df.industry != "."],
    "classification_fields": {
        "department": {
            "classification": location_classification,
            "level": "department"
        },
        "industry": {
            "classification": industry_classification,
            "level": "class"
        },
    },
    "digit_padding": {
        "department": 2,
        "industry": 4
    },
    "facet_fields": ["department", "industry", "year"],
    "facets": {
        ("department_id", "year"): {
            "employment": lambda x: x.sum(),
            "wages": lambda x: x.sum(),
        },
        ("industry_id", "year"): {
            "complexity": first
        },
        ("department_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "density": first,
            "cog": first,
            "coi": first,
            "rca": first
        }
    }
}

industry4digit_municipality = {
    "read_function": lambda: pd.read_stata(prefix_path("Atlas/Colombia/beta/PILA_andres/COL_PILA_ecomp-E_yir_2008-2012_rev3_mun.dta")),
    "field_mapping": pila_to_atlas_muni,
    "hook_pre_merge": lambda df: df[df.industry != "."],
    "classification_fields": {
        "municipality": {
            "classification": location_classification,
            "level": "municipality"
        },
        "industry": {
            "classification": industry_classification,
            "level": "class"
        },
    },
    "digit_padding": {
        "municipality": 5,
        "industry": 4
    },
    "facet_fields": ["municipality", "industry", "year"],
    "facets": {
        ("industry_id", "year"): {
            "complexity": first
        },
        ("municipality_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "density": first,
            "cog": first,
            "coi": first,
            "rca": first
        }
    }
}

population = {
    "read_function": lambda: pd.read_stata(prefix_path("Atlas/Colombia/beta/Final Metadata/col_pop_muni_dept_natl_1985_2013.dta")),
    "hook_pre_merge": lambda df: df[~df[["department", "year", "population"]].duplicated()],
    "field_mapping": {
        "year": "year",
        "dept_code": "department",
        "dept_pop": "population"
    },
    "classification_fields": {
        "department": {
            "classification": location_classification,
            "level": "department"
        },
    },
    "digit_padding": {
        "department": 2
    },
    "facet_fields": ["department", "year"],
    "facets": {
        ("department_id", "year"): {
            "population": first
        }
    }
}


gdp = {
    "read_function": lambda: pd.read_stata(prefix_path("Atlas/Colombia/beta/Final Metadata/col_nomgdp_muni_dept_natl_1990_2012.dta")),
    "field_mapping": {
        "dept_code": "department",
        "dept_gdp": "gdp_nominal",
        "muni_gdp": "gdp_real",
        "year": "year"
    },
    "classification_fields": {
        "department": {
            "classification": location_classification,
            "level": "department"
        },
    },
    "digit_padding": {
        "department": 2
    },
    "facet_fields": ["department", "year"],
    "facets": {
        ("department_id", "year"): {
            "gdp_real": first,
            "gdp_nominal": first,
        }
    }
}

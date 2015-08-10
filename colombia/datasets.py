import pandas as pd
import os.path

from linnaeus import classification

product_classification = classification.load("product/HS/Colombia_Prospedia/out/products_colombia_prospedia.csv")
location_classification = classification.load("location/Colombia/Prospedia/out/locations_colombia_prosperia.csv")
industry_classification = classification.load("industry/ISIC/Colombia_Prosperia/out/industries_colombia_isic_prosperia.csv")
country_classification = classification.load("location/International/DANE/out/locations_international_dane.csv")


country_classification.table.code = country_classification.table.code.astype(str).str.zfill(3)


def first(x):
    """Return first element of a group in a pandas GroupBy object"""
    return x.nth(0)


def sumGroup(x):
    """Get the sum for a pandas group by"""
    return x.sum()


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


trade4digit_rcpy_department = {
    "read_function": lambda: pd.read_stata(prefix_path("atlas/colombia/beta/trade/exp_rcpy_r2_p4.dta")),
    "field_mapping": {
        "r": "department",
        "ctry_dest": "country",
        "p": "product",
        "yr": "year",
        "X_rcpy_d": "export_value",
        "NP_rcpy": "num_plants"
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
        "country": {
            "classification": country_classification,
            "level": "country"
        },
    },
    "digit_padding": {
        "department": 2,
        "country": 3,
        "product": 4
    },
    "facet_fields": ["department", "country", "product", "year"],
    "facets": {
        ("country_id", "department_id", "product_id", "year"): {
            "export_value": first,
            "num_plants": first
        }
    }
}


trade4digit_rcpy_municipality = {
    "read_function": lambda: pd.read_stata(prefix_path("atlas/colombia/beta/trade/exp_rcpy_r5_p4.dta")),
    "field_mapping": {
        "r": "municipality",
        "ctry_dest": "country",
        "p": "product",
        "yr": "year",
        "X_rcpy_d": "export_value",
        "NP_rcpy": "num_plants"
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
        "country": {
            "classification": country_classification,
            "level": "country"
        },
    },
    "digit_padding": {
        "municipality": 5,
        "country": 3,
        "product": 4
    },
    "facet_fields": ["municipality", "country", "product", "year"],
    "facets": {
        ("country_id", "municipality_id", "product_id", "year"): {
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
            "employment": lambda x: x.sum(),
            "wages": lambda x: x.sum(),
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
    "hook_pre_merge": lambda df: df.drop_duplicates(["department", "year"]),
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


industry2digit_department = {
    "read_function": lambda: pd.read_stata(prefix_path("Atlas/Colombia/beta/Industries/output2008-2013_d2industrydescriptives.dta")),
    "hook_pre_merge": lambda df: df[df.industry != ""].drop_duplicates(["department", "industry", "year"]),
    "field_mapping": {

        "state_code": "department",
        "d2_code": "industry",
        "year": "year",
        "state_d2_establisments": "num_establishments",
        "state_d2_annualwages": "wages",
        "state_d2_employment": "employment"
    },
    "classification_fields": {
        "department": {
            "classification": location_classification,
            "level": "department"
        },
        "industry": {
            "classification": industry_classification,
            "level": "division"
        },
    },
    "digit_padding": {
        "department": 2,
        "industry": 2
    },
    "facet_fields": ["department", "industry", "year"],
    "facets": {
        ("industry_id", "year"): {
            "wages": sumGroup,
            "employment": sumGroup,
            "num_establishments": sumGroup,
        },

        ("department_id", "industry_id", "year"): {
            "wages": first,
            "employment": first,
            "num_establishments": first,
        }
    }
}

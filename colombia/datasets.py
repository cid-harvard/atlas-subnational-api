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


def load_trade4digit_department():
    prescriptives = pd.read_stata(prefix_path("Atlas/Colombia/beta/Trade/exp_ecomplexity_r2.dta"))

    exports = pd.read_stata(prefix_path("Atlas/Colombia/beta/Trade/exp_rpy_r2_p4.dta"))
    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "NP_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Atlas/Colombia/beta/Trade/imp_rpy_r2_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "NP_rpy": "import_num_plants"})
    imports = imports[imports.yr.between(2007, 2013)]

    descriptives = exports.merge(imports, on=["yr", "r", "p"], how="outer")
    descriptives = descriptives.fillna({
        "export_value": 0,
        "export_num_plants": 0,
        "import_value": 0,
        "import_num_plants": 0,
    })

    # TODO: ask moncho about products in df but not df2
    combo = prescriptives.merge(descriptives,
                                left_on=["yr", "r", "p4"],
                                right_on=["yr", "r", "p"])
    return combo

trade4digit_department = {
    "read_function": load_trade4digit_department,
    "field_mapping": {
        "r": "department",
        "p4": "product",
        "yr": "year",
        "export_value": "export_value",
        "export_num_plants": "export_num_plants",
        "import_value": "import_value",
        "import_num_plants": "import_num_plants",
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
            "export_value": sumGroup,
            "import_value": sumGroup,
            "export_num_plants": sumGroup,
            "import_num_plants": sumGroup
        },
        ("department_id", "product_id", "year"): {
            "export_value": first,
            "import_value": first,
            "export_num_plants": first,
            "import_num_plants": first,
            "export_rca": first,
            "density": first,
            "cog": first,
            "coi": first
        }
    }
}


def load_trade4digit_msa():
    prescriptives = pd.read_stata(prefix_path("Atlas/Colombia/beta/Trade/exp_ecomplexity_rcity.dta"))

    exports = pd.read_stata(prefix_path("Atlas/Colombia/beta/Trade/exp_rpy_ra_p4.dta"))
    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "NP_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Atlas/Colombia/beta/Trade/imp_rpy_ra_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "NP_rpy": "import_num_plants"})
    imports = imports[imports.yr.between(2007, 2013)]

    descriptives = exports.merge(imports, on=["yr", "r", "p"], how="outer")
    descriptives = descriptives.fillna({
        "export_value": 0,
        "export_num_plants": 0,
        "import_value": 0,
        "import_num_plants": 0,
    })

    # TODO: ask moncho about products in df but not df2
    combo = prescriptives.merge(descriptives,
                                left_on=["yr", "r", "p4"],
                                right_on=["yr", "r", "p"])
    return combo


trade4digit_msa = {
    "read_function": load_trade4digit_msa,
    "field_mapping": {
        "r": "msa",
        "p": "product",
        "yr": "year",
        "export_value": "export_value",
        "import_value": "import_value",
        "export_num_plants": "export_num_plants",
        "import_num_plants": "import_num_plants",
        "density_natl": "density",
        "eci_natl": "eci",
        "pci": "pci",
        "coi_natl": "coi",
        "cog_natl": "cog",
        "RCA_natl": "export_rca"
    },
    "classification_fields": {
        "msa": {
            "classification": location_classification,
            "level": "msa"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "product": 4
    },
    "facet_fields": ["msa", "product", "year"],
    "facets": {
        ("msa_id", "year"): {
            "eci": first,
        },
        ("msa_id", "product_id", "year"): {
            "export_value": first,
            "import_value": first,
            "export_num_plants": first,
            "import_num_plants": first,
            "export_rca": first,
            "density": first,
            "cog": first,
            "coi": first
        }
    }
}


def load_trade4digit_municipality():
    exports = pd.read_stata(prefix_path("Atlas/Colombia/beta/Trade/exp_rpy_r5_p4.dta"))
    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "NP_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Atlas/Colombia/beta/Trade/imp_rpy_r5_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "NP_rpy": "import_num_plants"})
    imports = imports[imports.yr.between(2007, 2013)]

    descriptives = exports.merge(imports, on=["yr", "r", "p"], how="outer")
    descriptives = descriptives.fillna({
        "export_value": 0,
        "export_num_plants": 0,
        "import_value": 0,
        "import_num_plants": 0,
    })

    return descriptives

trade4digit_municipality = {
    "read_function": load_trade4digit_municipality,
    "field_mapping": {
        "r": "municipality",
        "p": "product",
        "yr": "year",
        "export_value": "export_value",
        "export_num_plants": "export_num_plants",
        "import_value": "import_value",
        "import_num_plants": "import_num_plants",
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
            "export_num_plants": first,
            "import_value": first,
            "import_num_plants": first
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


industry4digit_department = {
    "read_function": lambda: pd.read_hdf(prefix_path("Atlas/Colombia/beta/Industries/industries_state.hdf"), "data"),
    "hook_pre_merge": lambda df: df.drop_duplicates(["department", "industry", "year"]),
    "field_mapping": {
        "state_code": "department",
        "p_code": "industry",
        "year": "year",
        "state_p_emp": "employment",
        "state_p_wage": "wages",
        "state_p_wagemonth": "monthly_wages",
        "state_p_est": "num_establishments",
        "state_p_rca": "rca",
        "state_p_distance_ps_pred": "density",
        "state_p_cog_ps_pred1": "cog",
        "all_p_pci": "complexity"
    },
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
            "monthly_wages": first,
            "num_establishments": sumGroup,
        },
        ("industry_id", "year"): {
            "employment": lambda x: x.sum(),
            "wages": lambda x: x.sum(),
            "monthly_wages": lambda x: x.sum(),
            "num_establishments": sumGroup,
            "complexity": first
        },
        ("department_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "monthly_wages": first,
            "num_establishments": first,
            "density": first,
            "cog": first,
            "rca": first
        }
    }
}


def hook_industry4digit_msa(df):
    df = df.drop_duplicates(["msa", "industry", "year"])
    df = df[df.msa.notnull()]
    df.msa = df.msa.astype(int).astype(str).str.zfill(5) + "0"
    return df

industry4digit_msa = {
    "read_function": lambda: pd.read_hdf(prefix_path("Atlas/Colombia/beta/Industries/industries_msa.hdf"), "data"),
    "hook_pre_merge": hook_industry4digit_msa,
    "field_mapping": {
        "msa_code": "msa",
        "p_code": "industry",
        "year": "year",
        "msa_p_emp": "employment",
        "msa_p_wage": "wages",
        "msa_p_wagemonth": "monthly_wages",
        "msa_p_rca": "rca",
        "msa_p_distance_hybrid": "density",
        "msa_p_cog_ps_pred1": "cog",
        "all_p_pci": "complexity"
    },
    "classification_fields": {
        "msa": {
            "classification": location_classification,
            "level": "msa"
        },
        "industry": {
            "classification": industry_classification,
            "level": "class"
        },
    },
    "digit_padding": {
        "industry": 4
    },
    "facet_fields": ["msa", "industry", "year"],
    "facets": {
        ("msa_id", "year"): {
            "employment": lambda x: x.sum(),
            "wages": lambda x: x.sum(),
        },
        ("industry_id", "year"): {
            "employment": lambda x: x.sum(),
            "wages": lambda x: x.sum(),
            "complexity": first
        },
        ("msa_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "monthly_wages": first,
            "density": first,
            "cog": first,
            "rca": first
        }
    }
}

industry4digit_municipality = {
    "read_function": lambda: pd.read_hdf(prefix_path("Atlas/Colombia/beta/Industries/industries_muni.hdf"), "data"),
    "hook_pre_merge": lambda df: df.drop_duplicates(["municipality", "industry", "year"]),
    "field_mapping": {
        "muni_code": "municipality",
        "p_code": "industry",
        "year": "year",
        "muni_p_emp": "employment",
        "muni_p_wage": "wages",
        "muni_p_wagemonth": "monthly_wages",
    },
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
        ("municipality_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "monthly_wages": first,
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


gdp_department = {
    "read_function": lambda: pd.read_stata(prefix_path("Atlas/Colombia/beta/Final Metadata/col_nomgdp_2000_2013.dta")),
    "hook_pre_merge": lambda df: df.drop_duplicates(["department", "year"]),
    "field_mapping": {
        "dept_code": "department",
        "dept_gdp": "gdp_nominal",
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
            "gdp_nominal": first,
        }
    }
}


industry2digit_department = {
    "read_function": lambda: pd.read_hdf(prefix_path("Atlas/Colombia/beta/Industries/industries_state.hdf"), "data"),
    "hook_pre_merge": lambda df: df.drop_duplicates(["department", "industry", "year"]),
    "field_mapping": {
        "state_code": "department",
        "d3_code": "industry",
        "year": "year",
        "state_d3_est": "num_establishments",
        "state_d3_wage": "wages",
        "state_d3_wagemonth": "monthly_wages",
        "state_d3_emp": "employment",
        #"state_d3_rca": "rca",
        "state_d3_distance_ps_pred1": "density",
        "state_d3_cog_ps_pred1": "cog",
        "all_d3_pci": "complexity"
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
            "monthly_wages": sumGroup,
            "employment": sumGroup,
            "num_establishments": sumGroup,
            "complexity": first
        },

        ("department_id", "industry_id", "year"): {
            "wages": first,
            "monthly_wages": first,
            "employment": first,
            "num_establishments": first,
            "density": first,
            "cog": first,
            #"rca": first
        }
    }
}

import pandas as pd
import os.path

from linnaeus import classification

product_classification = classification.load("product/HS/Colombia_Prospedia/out/products_colombia_prospedia.csv")
location_classification = classification.load("location/Colombia/Prospedia/out/locations_colombia_prosperia.csv")
industry_classification = classification.load("industry/ISIC/Colombia_Prosperia/out/industries_colombia_isic_prosperia.csv")
country_classification = classification.load("location/International/DANE/out/locations_international_dane.csv")
occupation_classification = classification.load("occupation/SOC/Colombia/out/occupations_soc_2010.csv")


country_classification.table.code = country_classification.table.code.astype(str).str.zfill(3)


def first(x):
    """Return first element of a group in a pandas GroupBy object"""
    return x.nth(0)


def sum_group(x):
    """Get the sum for a pandas group by"""
    return x.sum()


DATASET_ROOT = "/Users/makmana/ciddata/Subnationals/Atlas/Colombia/beta/"
YEAR_MIN_TRADE = 2007
YEAR_MAX_TRADE = 2013
YEAR_MIN_INDUSTRY = 2007
YEAR_MAX_INDUSTRY = 2013

# These are MSAs (Metropolitan Statistical Area) that have a single
# municipality associated with them - they're mostly "cities" which are munis
# that have population greater than a certain number (100k?). Alternatively it
# could have been that the way we generated MSAs (looking at commute patterns
# between cities) could have generated a MSA that has only one city, but I
# don't think this is the case. These values are from Moncho's Trade dataset,
# in Keys/Colombia_cities_of_onemuni_key.dta
SINGLE_MUNI_MSAS = ['73001', '47001', '23001', '20001', '76109', '41001', '76520',
                    '19001', '70001', '44001', '68081', '52835', '18001', '05045',
                    '44430', '05837', '76147', '85001', '13430', '25290', '76111',
                    '27001', '23417', '41551', '47189', '05154', '54498', '20011',
                    '23162', '19698', '81001', '73268', '17380', '23466', '13244',
                    '88001', '05172', '50006', '15176', '70215', '47288', '50313',
                    '54518']


def prefix_path(to_prefix):
    return os.path.join(DATASET_ROOT, to_prefix)


def load_trade4digit_country():
    prescriptives = pd.read_stata(prefix_path("Trade/exp_ecomplexity_rc.dta"))

    exports = pd.read_stata(prefix_path("Trade/exp_rpy_rc_p4.dta"))
    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "NP_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Trade/imp_rpy_rc_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "NP_rpy": "import_num_plants"})

    descriptives = exports.merge(imports, on=["yr", "r", "p"], how="outer")
    descriptives = descriptives.fillna({
        "export_value": 0,
        "export_num_plants": 0,
        "import_value": 0,
        "import_num_plants": 0,
    })

    combo = prescriptives.merge(descriptives,
                                left_on=["yr", "r", "p4"],
                                right_on=["yr", "r", "p"])

    combo = combo[combo.yr.between(YEAR_MIN_TRADE, YEAR_MAX_TRADE)]
    combo["r"] = "COL"
    return combo

trade4digit_country = {
    "read_function": load_trade4digit_country,
    "field_mapping": {
        "r": "location",
        "p4": "product",
        "yr": "year",
        "export_value": "export_value",
        "export_num_plants": "export_num_plants",
        "import_value": "import_value",
        "import_num_plants": "import_num_plants",
        "density_intl": "density",
        "eci_intl": "eci",
        "pci": "pci",
        "coi_intl": "coi",
        "cog_intl": "cog",
        "RCA_intl": "export_rca"
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "country"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "location": 1,
        "product": 4
    },
    "facet_fields": ["location", "product", "year"],
    "facets": {
        ("location_id", "product_id", "year"): {
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


def load_trade4digit_department():
    prescriptives = pd.read_stata(prefix_path("Trade/exp_ecomplexity_r2.dta"))

    exports = pd.read_stata(prefix_path("Trade/exp_rpy_r2_p4.dta"))
    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "NP_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Trade/imp_rpy_r2_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "NP_rpy": "import_num_plants"})

    descriptives = exports.merge(imports, on=["yr", "r", "p"], how="outer")
    descriptives = descriptives.fillna({
        "export_value": 0,
        "export_num_plants": 0,
        "import_value": 0,
        "import_num_plants": 0,
    })

    combo = prescriptives.merge(descriptives,
                                left_on=["yr", "r", "p4"],
                                right_on=["yr", "r", "p"])
    combo = combo[combo.yr.between(YEAR_MIN_TRADE, YEAR_MAX_TRADE)]
    return combo

trade4digit_department = {
    "read_function": load_trade4digit_department,
    "field_mapping": {
        "r": "location",
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
        "location": {
            "classification": location_classification,
            "level": "department"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "location": 2,
        "product": 4
    },
    "facet_fields": ["location", "product", "year"],
    "facets": {
        ("location_id", "year"): {
            "eci": first,
        },
        ("product_id", "year"): {
            "pci": first,
            "export_value": sum_group,
            "import_value": sum_group,
            "export_num_plants": sum_group,
            "import_num_plants": sum_group
        },
        ("location_id", "product_id", "year"): {
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
    prescriptives = pd.read_stata(prefix_path("Trade/exp_ecomplexity_rcity.dta"))

    # Fix certain muni codes to msa codes, see MEX-148
    is_single_muni_msa = prescriptives.r.isin(SINGLE_MUNI_MSAS)
    prescriptives.loc[is_single_muni_msa, "r"] = prescriptives.loc[is_single_muni_msa, "r"].map(lambda x: x + "0")

    exports = pd.read_stata(prefix_path("Trade/exp_rpy_ra_p4.dta"))

    # Add missing exports from single muni MSAs. See MEX-148
    muni_exports = pd.read_stata(prefix_path("Trade/exp_rpy_r5_p4.dta"))
    muni_exports = muni_exports[muni_exports.r.isin(SINGLE_MUNI_MSAS)]
    muni_exports.r = muni_exports.r.map(lambda x: x + "0")
    exports = pd.concat([exports, muni_exports]).reset_index(drop=True)

    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "NP_rpy": "export_num_plants"})

    imports = pd.read_stata(prefix_path("Trade/imp_rpy_ra_p4.dta"))

    # Add missing imports from single muni MSAs. See MEX-148
    muni_imports = pd.read_stata(prefix_path("Trade/imp_rpy_r5_p4.dta"))
    muni_imports = muni_imports[muni_imports.r.isin(SINGLE_MUNI_MSAS)]
    muni_imports.r = muni_imports.r.map(lambda x: x + "0")
    imports = pd.concat([imports, muni_imports]).reset_index(drop=True)

    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "NP_rpy": "import_num_plants"})

    descriptives = exports.merge(imports, on=["yr", "r", "p"], how="outer")
    descriptives = descriptives.fillna({
        "export_value": 0,
        "export_num_plants": 0,
        "import_value": 0,
        "import_num_plants": 0,
    })

    combo = prescriptives.merge(descriptives,
                                left_on=["yr", "r", "p4"],
                                right_on=["yr", "r", "p"])
    combo = combo[combo.yr.between(YEAR_MIN_TRADE, YEAR_MAX_TRADE)]
    return combo


trade4digit_msa = {
    "read_function": load_trade4digit_msa,
    "field_mapping": {
        "r": "location",
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
        "location": {
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
    "facet_fields": ["location", "product", "year"],
    "facets": {
        ("location_id", "year"): {
            "eci": first,
        },
        ("product_id", "year"): {
            "pci": first,
        },
        ("location_id", "product_id", "year"): {
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
    exports = pd.read_stata(prefix_path("Trade/exp_rpy_r5_p4.dta"))
    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "NP_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Trade/imp_rpy_r5_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "NP_rpy": "import_num_plants"})

    descriptives = exports.merge(imports, on=["yr", "r", "p"], how="outer")
    descriptives = descriptives.fillna({
        "export_value": 0,
        "export_num_plants": 0,
        "import_value": 0,
        "import_num_plants": 0,
    })

    descriptives = descriptives[descriptives.yr.between(YEAR_MIN_TRADE, YEAR_MAX_TRADE)]
    return descriptives

trade4digit_municipality = {
    "read_function": load_trade4digit_municipality,
    "field_mapping": {
        "r": "location",
        "p": "product",
        "yr": "year",
        "export_value": "export_value",
        "export_num_plants": "export_num_plants",
        "import_value": "import_value",
        "import_num_plants": "import_num_plants",
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "municipality"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "location": 5,
        "product": 4
    },
    "facet_fields": ["location", "product", "year"],
    "facets": {
        ("location_id", "product_id", "year"): {
            "export_value": first,
            "export_num_plants": first,
            "import_value": first,
            "import_num_plants": first
        }
    }
}


trade4digit_rcpy_fields_export = {
    "r": "location",
    "country": "country",
    "p": "product",
    "yr": "year",
    "X_rcpy_d_export": "export_value",
    "NP_rcpy_export": "export_num_plants",
    "X_rcpy_d_import": "import_value",
    "NP_rcpy_import": "import_num_plants"
}


def read_trade4digit_rcpy(suffix="rc_p4"):
    e = pd\
        .read_stata(prefix_path("Trade/exp_rcpy_{}.dta".format(suffix)))\
        .rename(columns={"ctry_dest": "country"})
    i = pd\
        .read_stata(prefix_path("Trade/imp_rcpy_{}.dta".format(suffix)))\
        .rename(columns={"ctry_orig": "country"})
    df = e.merge(i,
                 on=['r', 'p', 'country', 'yr'],
                 how='outer',
                 suffixes=('_export', '_import'))
    df = df[df.yr.between(YEAR_MIN_TRADE, YEAR_MAX_TRADE)]
    return df.fillna(0)


def replace_country(df):
    df["r"] = "COL"
    return df

trade4digit_rcpy_country = {
    "read_function": lambda: replace_country(read_trade4digit_rcpy(suffix="rc_p4")),
    "field_mapping": trade4digit_rcpy_fields_export,
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "country"
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
        "country": 3,
        "product": 4
    },
    "facet_fields": ["location", "country", "product", "year"],
    "facets": {
        ("country_id", "location_id", "year"): {
            "export_value": sum_group,
            "export_num_plants": sum_group,
            "import_value": sum_group,
            "import_num_plants": sum_group,
        },
        ("product_id", "country_id", "year"): {
            "export_value": sum_group,
            "export_num_plants": sum_group,
            "import_value": sum_group,
            "import_num_plants": sum_group,
        },
        ("country_id", "location_id", "product_id", "year"): {
            "export_value": first,
            "export_num_plants": first,
            "import_value": first,
            "import_num_plants": first,
        }
    }
}


trade4digit_rcpy_department = {
    "read_function": lambda: read_trade4digit_rcpy(suffix="r2_p4"),
    "field_mapping": trade4digit_rcpy_fields_export,
    "classification_fields": {
        "location": {
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
        "location": 2,
        "country": 3,
        "product": 4
    },
    "facet_fields": ["location", "country", "product", "year"],
    "facets": {
        ("country_id", "location_id", "year"): {
            "export_value": sum_group,
            "export_num_plants": sum_group,
            "import_value": sum_group,
            "import_num_plants": sum_group,
        },
        ("country_id", "location_id", "product_id", "year"): {
            "export_value": first,
            "export_num_plants": first,
            "import_value": first,
            "import_num_plants": first,
        }
    }
}


trade4digit_rcpy_msa = {
    "read_function": lambda: read_trade4digit_rcpy(suffix="ra_p4"),
    "field_mapping": trade4digit_rcpy_fields_export,
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "msa"
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
        "country": 3,
        "product": 4
    },
    "facet_fields": ["location", "country", "product", "year"],
    "facets": {
        ("country_id", "location_id", "year"): {
            "export_value": sum_group,
            "export_num_plants": sum_group,
            "import_value": sum_group,
            "import_num_plants": sum_group,
        },
        ("country_id", "location_id", "product_id", "year"): {
            "export_value": first,
            "export_num_plants": first,
            "import_value": first,
            "import_num_plants": first,
        }
    }
}

trade4digit_rcpy_municipality = {
    "read_function": lambda: read_trade4digit_rcpy(suffix="r5_p4"),
    "field_mapping": trade4digit_rcpy_fields_export,
    "classification_fields": {
        "location": {
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
        "location": 5,
        "country": 3,
        "product": 4
    },
    "facet_fields": ["location", "country", "product", "year"],
    "facets": {
        ("country_id", "location_id", "product_id", "year"): {
            "export_value": first,
            "export_num_plants": first,
            "import_value": first,
            "import_num_plants": first,
        },
        ("country_id", "location_id", "year"): {
            "export_value": sum_group,
            "export_num_plants": sum_group,
            "import_value": sum_group,
            "import_num_plants": sum_group,
        }
    }
}


def hook_industry(df):
    df = df.drop_duplicates(["location", "industry", "year"])
    df = df[df.location.notnull()]
    df = df[df.year.between(YEAR_MIN_INDUSTRY, YEAR_MAX_INDUSTRY)]
    return df


def industry4digit_country_read():
    df = pd.read_hdf(prefix_path("Industries/industries_all.hdf"), "data")
    df["state_code"] = "COL"
    return df

industry4digit_country = {
    "read_function": industry4digit_country_read,
    "field_mapping": {
        "state_code": "location",
        "p_code": "industry",
        "year": "year",
        "all_p_emp": "employment",
        "all_p_wage": "wages",
        "all_p_wagemonth": "monthly_wages",
        "all_p_est": "num_establishments",
    },
    "hook_pre_merge": hook_industry,
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "country"
        },
        "industry": {
            "classification": industry_classification,
            "level": "class"
        },
    },
    "digit_padding": {
        "location": 1,
        "industry": 4
    },
    "facet_fields": ["location", "industry", "year"],
    "facets": {
        ("location_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "monthly_wages": first,
            "num_establishments": first,
        }
    }
}

industry4digit_department = {
    "read_function": lambda: pd.read_hdf(prefix_path("Industries/industries_state.hdf"), "data"),
    "field_mapping": {
        "state_code": "location",
        "p_code": "industry",
        "year": "year",
        "state_p_emp": "employment",
        "state_p_wage": "wages",
        "state_p_wagemonth": "monthly_wages",
        "state_p_est": "num_establishments",
        "state_p_rca": "rca",
        "state_p_distance_ps": "distance",
        "state_p_cog_ps_pred1": "cog",
        "all_p_pci": "complexity"
    },
    "hook_pre_merge": hook_industry,
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "department"
        },
        "industry": {
            "classification": industry_classification,
            "level": "class"
        },
    },
    "digit_padding": {
        "location": 2,
        "industry": 4
    },
    "facet_fields": ["location", "industry", "year"],
    "facets": {
        ("location_id", "year"): {
            "employment": sum_group,
            "wages": sum_group,
            "monthly_wages": first,
            "num_establishments": sum_group,
        },
        ("industry_id", "year"): {
            "employment": sum_group,
            "wages": sum_group,
            "monthly_wages": sum_group,
            "num_establishments": sum_group,
            "complexity": first
        },
        ("location_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "monthly_wages": first,
            "num_establishments": first,
            "distance": first,
            "cog": first,
            "rca": first
        }
    }
}


def hook_industry4digit_msa(df):
    df = hook_industry(df)
    df.location = df.location.astype(int).astype(str).str.zfill(5) + "0"
    return df

industry4digit_msa = {
    "read_function": lambda: pd.read_hdf(prefix_path("Industries/industries_msa.hdf"), "data"),
    "hook_pre_merge": hook_industry4digit_msa,
    "field_mapping": {
        "msa_code": "location",
        "p_code": "industry",
        "year": "year",
        "msa_p_emp": "employment",
        "msa_p_wage": "wages",
        "msa_p_wagemonth": "monthly_wages",
        "msa_p_rca": "rca",
        "msa_p_distance_hybrid": "distance",
        "msa_p_cog_ps_pred1": "cog",
        "all_p_pci": "complexity"
    },
    "classification_fields": {
        "location": {
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
    "facet_fields": ["location", "industry", "year"],
    "facets": {
        ("location_id", "year"): {
            "employment": sum_group,
            "wages": sum_group,
        },
        ("industry_id", "year"): {
            "employment": sum_group,
            "wages": sum_group,
            "complexity": first
        },
        ("location_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "monthly_wages": first,
            "distance": first,
            "cog": first,
            "rca": first
        }
    }
}

industry4digit_municipality = {
    "read_function": lambda: pd.read_hdf(prefix_path("Industries/industries_muni.hdf"), "data"),
    "hook_pre_merge": hook_industry,
    "field_mapping": {
        "muni_code": "location",
        "p_code": "industry",
        "year": "year",
        "muni_p_emp": "employment",
        "muni_p_wage": "wages",
        "muni_p_wagemonth": "monthly_wages",
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "municipality"
        },
        "industry": {
            "classification": industry_classification,
            "level": "class"
        },
    },
    "digit_padding": {
        "location": 5,
        "industry": 4
    },
    "facet_fields": ["location", "industry", "year"],
    "facets": {
        ("location_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "monthly_wages": first,
        }
    }
}

population = {
    "read_function": lambda: pd.read_stata(prefix_path("Final Metadata/col_pop_muni_dept_natl_1985_2013.dta")),
    "hook_pre_merge": lambda df: df[~df[["location", "year", "population"]].duplicated()],
    "field_mapping": {
        "year": "year",
        "dept_code": "location",
        "dept_pop": "population"
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "department"
        },
    },
    "digit_padding": {
        "location": 2
    },
    "facet_fields": ["location", "year"],
    "facets": {
        ("location_id", "year"): {
            "population": first
        }
    }
}


gdp_nominal_department = {
    "read_function": lambda: pd.read_stata(prefix_path("Final Metadata/col_nomgdp_2000_2013.dta")),
    "hook_pre_merge": lambda df: df.drop_duplicates(["location", "year"]),
    "field_mapping": {
        "dept_code": "location",
        "dept_gdp": "gdp_nominal",
        "year": "year"
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "department"
        },
    },
    "digit_padding": {
        "location": 2
    },
    "facet_fields": ["location", "year"],
    "facets": {
        ("location_id", "year"): {
            "gdp_nominal": first,
        }
    }
}


gdp_real_department = {
    "read_function": lambda: pd.read_stata(prefix_path("Final Metadata/col_realgdp_dept_natl_2000_2013.dta")),
    "field_mapping": {
        "dept_code": "location",
        "real_gdp": "gdp_real",
        "year": "year"
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "department"
        },
    },
    "digit_padding": {
        "location": 2
    },
    "facet_fields": ["location", "year"],
    "facets": {
        ("location_id", "year"): {
            "gdp_real": first,
        }
    }
}


industry2digit_department = {
    "read_function": lambda: pd.read_hdf(prefix_path("Industries/industries_state.hdf"), "data"),
    "hook_pre_merge": hook_industry,
    "field_mapping": {
        "state_code": "location",
        "d3_code": "industry",
        "year": "year",
        "state_d3_est": "num_establishments",
        "state_d3_wage": "wages",
        "state_d3_wagemonth": "monthly_wages",
        "state_d3_emp": "employment",
        "state_d3_rca": "rca",
        "state_d3_distance_ps_pred1": "distance",
        "state_d3_cog_ps_pred1": "cog",
        "all_d3_pci": "complexity"
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "department"
        },
        "industry": {
            "classification": industry_classification,
            "level": "division"
        },
    },
    "digit_padding": {
        "location": 2,
        "industry": 2
    },
    "facet_fields": ["location", "industry", "year"],
    "facets": {
        ("industry_id", "year"): {
            "wages": sum_group,
            "monthly_wages": sum_group,
            "employment": sum_group,
            "num_establishments": sum_group,
            "complexity": first
        },

        ("location_id", "industry_id", "year"): {
            "wages": first,
            "monthly_wages": first,
            "employment": first,
            "num_establishments": first,
            "distance": first,
            "cog": first,
            "rca": first
        }
    }
}


def hook_industry2digit_msa(df):
    df = hook_industry(df)
    df.location = df.location.astype(int).astype(str).str.zfill(5) + "0"
    return df

industry2digit_msa = {
    "read_function": lambda: pd.read_hdf(prefix_path("Industries/industries_msa.hdf"), "data"),
    "hook_pre_merge": hook_industry2digit_msa,
    "field_mapping": {
        "msa_code": "location",
        "d3_code": "industry",
        "year": "year",
        "msa_d3_est": "num_establishments",
        "msa_d3_wage": "wages",
        "msa_d3_wagemonth": "monthly_wages",
        "msa_d3_emp": "employment",
        "msa_d3_rca": "rca",
        "msa_d3_distance_ps_pred1": "distance",
        "msa_d3_cog_ps_pred1": "cog",
        "all_d3_pci": "complexity"
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "msa"
        },
        "industry": {
            "classification": industry_classification,
            "level": "division"
        },
    },
    "digit_padding": {
        "industry": 2,
        "location": 5
    },
    "facet_fields": ["location", "industry", "year"],
    "facets": {
        ("industry_id", "year"): {
            "wages": sum_group,
            "monthly_wages": sum_group,
            "employment": sum_group,
            "num_establishments": sum_group,
            "complexity": first
        },

        ("location_id", "industry_id", "year"): {
            "wages": first,
            "monthly_wages": first,
            "employment": first,
            "num_establishments": first,
            "distance": first,
            "cog": first,
            "rca": first
        }
    }
}

occupation2digit_industry2digit = {
    "read_function": lambda: pd.read_stata(prefix_path("Vacancies/Vacancies_do010_2d-Ind_X_4d-Occ.dta")),
    "field_mapping": {
        "onet_4dig": "occupation",
        "ciiu_2dig": "industry",
        "num_vacantes": "num_vacancies",
        "wage_mean": "average_wages"
    },
    "classification_fields": {
        "occupation": {
            "classification": occupation_classification,
            "level": "minor_group"
        },
        "industry": {
            "classification": industry_classification,
            "level": "division"
        },
    },
    "digit_padding": {
        "occupation": 7,
        "industry": 4
    },
    "facet_fields": ["occupation", "industry"],
    "facets": {
        ("occupation_id", "industry_id"): {
            "average_wages": first,
            "num_vacancies": first,
        }
    }
}

occupation2digit = {
    "read_function": lambda: pd.read_stata(prefix_path("Vacancies/Vacancies_do010_4d-Occ.dta")),
    "field_mapping": {
        "onet_4dig": "occupation",
        "num_vacantes": "num_vacancies",
        "wage_mean": "average_wages"
    },
    "classification_fields": {
        "occupation": {
            "classification": occupation_classification,
            "level": "minor_group"
        },
    },
    "digit_padding": {
        "occupation": 7,
    },
    "facet_fields": ["occupation"],
    "facets": {
        ("occupation_id"): {
            "average_wages": first,
            "num_vacancies": first,
        }
    }
}

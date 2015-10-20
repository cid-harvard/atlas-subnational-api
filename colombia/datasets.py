import pandas as pd
import os.path

from linnaeus import classification

product_classification = classification.load("product/HS/Colombia_Prospedia/out/products_colombia_prospedia.csv")
location_classification = classification.load("location/Mexico/INEGI/out/locations_mexico_inegi.csv")
industry_classification = classification.load("industry/NAICS/Mexico_datlas/out/industries_mexico_scian_2007_datlas.csv")
country_classification = classification.load("location/International/DANE/out/locations_international_dane.csv")
occupation_classification = classification.load("occupation/SOC/Colombia/out/occupations_soc_2010.csv")


country_classification.table.code = country_classification.table.code.astype(str).str.zfill(3)


def first(x):
    """Return first element of a group in a pandas GroupBy object"""
    return x.nth(0)


def sum_group(x):
    """Get the sum for a pandas group by"""
    return x.sum()


DATASET_ROOT = "/Users/makmana/ciddata/Subnationals/Atlas/Mexico/beta/"


def prefix_path(to_prefix):
    return os.path.join(DATASET_ROOT, to_prefix)


def load_trade4digit_country():
    prescriptives = pd.read_stata(prefix_path("Trade/exp_ecomplexity_rm.dta"))

    exports = pd.read_stata(prefix_path("Trade/exp_rpy_rm_p4.dta"))
    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "O_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Trade/imp_rpy_rm_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "O_rpy": "import_num_plants"})

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

    combo["r"] = "MEX"
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
                                      "O_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Trade/imp_rpy_r2_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "O_rpy": "import_num_plants"})

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
        "r": "location",
        "p": "product",
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
    prescriptives = pd.read_stata(prefix_path("Trade/exp_ecomplexity_ra.dta"))

    exports = pd.read_stata(prefix_path("Trade/exp_rpy_ra_p4.dta"))
    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "O_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Trade/imp_rpy_ra_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "O_rpy": "import_num_plants"})

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
                                      "O_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Trade/imp_rpy_r5_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "O_rpy": "import_num_plants"})

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


trade4digit_rcpy_department = {
    "read_function": lambda: pd.read_stata(prefix_path("Trade/exp_rcpy_r2_p4.dta")),
    "field_mapping": {
        "r": "location",
        "ctry_dest": "country",
        "p": "product",
        "yr": "year",
        "X_rcpy_d": "export_value",
        "NP_rcpy": "num_plants"
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
        ("country_id", "location_id", "product_id", "year"): {
            "export_value": first,
            "num_plants": first
        }
    }
}


trade4digit_rcpy_municipality = {
    "read_function": lambda: pd.read_stata(prefix_path("Trade/exp_rcpy_r5_p4.dta")),
    "field_mapping": {
        "r": "location",
        "ctry_dest": "country",
        "p": "product",
        "yr": "year",
        "X_rcpy_d": "export_value",
        "NP_rcpy": "num_plants"
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
            "num_plants": first
        }
    }
}


def industry4digit_country_read():
    df = pd.read_hdf(prefix_path("Industries/industries_all.hdf"), "data")
    df["state_code"] = "MEX"
    return df

industry4digit_country = {
    "read_function": industry4digit_country_read ,
    "field_mapping": {
        "state_code": "location",
        "p_code": "industry",
        "year": "year",
        "all_p_emp": "employment",
        "all_p_wage": "wages",
        #"all_p_wagemonth": "monthly_wages",
        "all_p_est": "num_establishments",
    },
    "hook_pre_merge": lambda df: df.drop_duplicates(["location", "industry", "year"]),
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
            #"monthly_wages": first,
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
        "state_p_est": "num_establishments",
        "state_p_rca": "rca",
        "state_p_distance_ps_pred1": "distance",
        "state_p_cog_ps_pred1": "cog",
        "all_p_pci": "complexity"
    },
    "hook_pre_merge": lambda df: df.drop_duplicates(["location", "industry", "year"]),
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
            "num_establishments": sum_group,
        },
        ("industry_id", "year"): {
            "employment": sum_group,
            "wages": sum_group,
            "num_establishments": sum_group,
            "complexity": first
        },
        ("location_id", "industry_id", "year"): {
            "employment": first,
            "wages": first,
            "num_establishments": first,
            "distance": first,
            "cog": first,
            "rca": first
        }
    }
}


def hook_industry4digit_msa(df):
    df = df.drop_duplicates(["location", "industry", "year"])
    df = df[df.location.notnull()]
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
        #"msa_p_wagemonth": "monthly_wages",
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
        "location": 2,
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
            #"monthly_wages": first,
            "distance": first,
            "cog": first,
            "rca": first
        }
    }
}

industry4digit_municipality = {
    "read_function": lambda: pd.read_hdf(prefix_path("Industries/industries_muni.hdf"), "data"),
    "hook_pre_merge": lambda df: df.drop_duplicates(["location", "industry", "year"]),
    "field_mapping": {
        "muni_code": "location",
        "p_code": "industry",
        "year": "year",
        "muni_p_emp": "employment",
        "muni_p_wage": "wages",
        #"muni_p_wagemonth": "monthly_wages",
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
            #"monthly_wages": first,
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
    "hook_pre_merge": lambda df: df.drop_duplicates(["location", "industry", "year"]),
    "field_mapping": {
        "state_code": "location",
        "d2_code": "industry",
        "year": "year",
        "state_d2_est": "num_establishments",
        "state_d2_wage": "wages",
        #"state_d2_wagemonth": "monthly_wages",
        "state_d2_emp": "employment",
        #"state_d2_rca": "rca",
        "state_d2_distance_ps_pred1": "distance",
        "state_d2_cog_ps_pred1": "cog",
        "all_d2_pci": "complexity"
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
            #"monthly_wages": sum_group,
            "employment": sum_group,
            "num_establishments": sum_group,
            "complexity": first
        },

        ("location_id", "industry_id", "year"): {
            "wages": first,
            #"monthly_wages": first,
            "employment": first,
            "num_establishments": first,
            "distance": first,
            "cog": first,
            #"rca": first
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

# vim: set fileencoding=utf8

import pandas as pd
import numpy as np


from colombia import models, create_app
from colombia.core import db

from dataset_tools import (process_dataset, translate_columns, cut_columns,
                           fillin, merge_to_table)
from datasets import trade4digit_department


def classification_to_models(classification, model):
    models = []
    for index, row in classification.table.iterrows():
        row = row.replace([np.nan], [None])
        m = model()
        m.id = index.item()
        m.code = row["code"]
        m.name_en = row["name"]
        m.level = row["level"]
        m.parent_id = row["parent_id"]
        models.append(m)
    return models

aduanas_to_atlas = {
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
}

gdp_to_atlas = {
    "depcode": "department",
    "depgdpn": "gdp_nominal",
    "gdpkmultipliedbydeflator": "gdp_real",
    "year": "year"
}


pop_to_atlas = {
    "year": "year",
    "dp": "department",
    "popdept": "population"
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


if __name__ == "__main__":

        app = create_app()
        with app.app_context():

            # Load classifications
            from datasets import (product_classification,
                                  industry_classification,
                                  location_classification)

            products = classification_to_models(product_classification,
                                                models.HSProduct)
            db.session.add_all(products)
            db.session.commit()

            locations = classification_to_models(location_classification,
                                                models.Location)
            db.session.add_all(locations)
            db.session.commit()

            industries = classification_to_models(industry_classification,
                                                  models.Industry)
            db.session.add_all(industries)
            db.session.commit()

            # Department product year
            df = pd.read_stata("/Users/makmana/ciddata/Aduanas/exp_ecomplexity_dpto_oldstata.dta")
            df = translate_columns(df, aduanas_to_atlas)
            df = cut_columns(df, aduanas_to_atlas.values())
            df["product"] = df["product"].astype(int).astype(str).str.zfill(4)


            ret = process_dataset(trade4digit_department)


            df = fillin(df, ["department", "product", "year"]).reset_index()

            df = merge_to_table(product_classification.level("4digit"),
                                "product_id", df, "product")
            df = merge_to_table(location_classification.level("department"),
                                "department_id", df, "department")


            py = df.groupby(["product_id", "year"])[["pci"]].first().reset_index()
            py.to_sql("product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # GDP data
            gdp_df = pd.read_stata("/Users/makmana/ciddata/metadata/Annual GDP (nominal)/COL_nomrealgdp_dept_annual1990-2012.dta")
            gdp_df = translate_columns(gdp_df, gdp_to_atlas)
            gdp_df = cut_columns(gdp_df, gdp_to_atlas.values())

            gdp_df.gdp_real = gdp_df.gdp_real * (10 ** 6)
            gdp_df.gdp_nominal = gdp_df.gdp_nominal * (10 ** 6)
            gdp_df.department = gdp_df.department.astype(str).str.zfill(2)

            # Pop data
            pop_df = pd.read_stata("/Users/makmana/ciddata/metadata/Population/COL_pop_deptmunicip_1985-2012.dta")
            pop_df = translate_columns(pop_df, pop_to_atlas)
            pop_df = cut_columns(pop_df, pop_to_atlas.values())

            pop_df = pop_df.groupby(["department", "year"])[["population"]].first().reset_index()

            pop_df.department = pop_df.department.astype(str).str.zfill(2)
            pop_df = pop_df[(2007 <= pop_df.year) & (pop_df.year <= 2013)]

            cy = df.groupby(["department", "year"])[["eci", "department_id"]].first().reset_index()
            cy = cy.merge(gdp_df,
                          on=["department", "year"],
                          how="left")
            cy = cy.merge(pop_df,
                          on=["department", "year"],
                          how="left")
            cy["gdp_pc_real"] = cy.gdp_real / cy.population
            cy["gdp_pc_nominal"] = cy.gdp_nominal / cy.population

            # TODO: In the real thing, we won't need this "department" column
            # here because we won't need to merge based on code because
            # everything will have been converted to IDs already.
            cy = cy.drop("department", axis=1)
            cy.to_sql("department_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            df = df.groupby(["department_id", "product_id", "year"])[["export_value", "export_rca", "density", "cog", "coi"]].first().reset_index()
            df.to_sql("department_product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            import ipdb; ipdb.set_trace()

            # Department - industry - year
            df = pd.read_stata("/Users/makmana/ciddata/PILA_andres/COL_PILA_ecomp-E_yir_2008-2012_rev3_dpto.dta")
            df = translate_columns(df, pila_to_atlas)
            df = cut_columns(df, pila_to_atlas.values())

            df = df[df.industry != "."]

            df = fillin(df, ["department", "industry", "year"]).reset_index()

            df = merge_to_table(industry_classification.level("class"),
                                "industry_id", df, "industry")
            df = merge_to_table(location_classification.level("department"),
                                "department_id", df, "department")

            # Industry - Year
            iy = df.groupby(["industry_id", "year"])[["complexity"]].first().reset_index()
            iy.to_sql("industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Department - industry - year
            df = df.groupby(["department_id", "industry_id", "year"])[["employment", "wages", "rca", "density", "cog", "coi"]].first().reset_index()
            df.to_sql("department_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")


            # Municipality - industry - year
            df = pd.read_stata("/Users/makmana/ciddata/PILA_andres/COL_PILA_ecomp-E_yir_2008-2012_rev3_mun.dta")
            df = translate_columns(df, pila_to_atlas_muni)
            df = cut_columns(df, pila_to_atlas_muni.values())

            df = df[df.industry != "."]

            df = fillin(df, ["municipality", "industry", "year"]).reset_index()

            df = merge_to_table(industry_classification.level("class"),
                                "industry_id", df, "industry")
            df = merge_to_table(location_classification.level("municipality"),
                                "municipality_id", df, "municipality")

            df = df.groupby(["municipality_id", "industry_id", "year"])[["employment", "wages", "rca", "density", "cog", "coi"]].first().reset_index()
            df.to_sql("municipality_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

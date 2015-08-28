from colombia import create_app
from dataset_tools import process_dataset, merge_classification_by_id
from datasets import (trade4digit_department, trade4digit_msa,
                      trade4digit_municipality, industry4digit_department,
                      industry4digit_msa, industry2digit_department,
                      industry4digit_municipality,
                      trade4digit_rcpy_municipality,
                      occupation2digit_industry2digit,
                      gdp_nominal_department,
                      gdp_real_department, population
                      )

from datasets import (product_classification,
                      industry_classification,
                      location_classification,
                      country_classification,
                      occupation_classification
                      )

import os

def save_products_department():
    ret = process_dataset(trade4digit_department)

    dpy = ret[('department_id', 'product_id', 'year')].reset_index()
    py = ret[('product_id', 'year')][["pci"]].reset_index()
    dy = ret[('department_id', 'year')][["eci"]].reset_index()

    m = dpy.merge(py, on=["product_id", "year"])
    m = m.merge(dy, on=["department_id", "year"])
    m = merge_classification_by_id(m, location_classification,
                                   "department_id", "location")
    m = merge_classification_by_id(m, product_classification,
                                   "product_id", "product")
    return m


def save_products_msa():
    ret = process_dataset(trade4digit_msa)

    df = ret[('msa_id', 'product_id', 'year')].reset_index()
    py = ret[('product_id', 'year')][["pci"]].reset_index()
    dy = ret[('msa_id', 'year')][["eci"]].reset_index()

    df = df.merge(py, on=["product_id", "year"])
    df = df.merge(dy, on=["msa_id", "year"])

    df = merge_classification_by_id(df, location_classification,
                                    "msa_id", "location")
    df = merge_classification_by_id(df, product_classification, "product_id",
                                    "product")
    return df


def save_products_muni():
    ret = process_dataset(trade4digit_municipality)

    df = ret[('municipality_id', 'product_id', 'year')].reset_index()
    df = merge_classification_by_id(df, location_classification,
                                    "municipality_id", "location")
    df = merge_classification_by_id(df, product_classification, "product_id",
                                    "product")
    return df


def save_industries_department():
    ret = process_dataset(industry4digit_department)

    dpy = ret[('department_id', 'industry_id', 'year')].reset_index()
    py = ret[('industry_id', 'year')][["complexity"]].reset_index()

    m = dpy.merge(py, on=["industry_id", "year"])
    m = merge_classification_by_id(m, location_classification,
                                   "department_id", "location")
    m = merge_classification_by_id(m, industry_classification,
                                   "industry_id", "industry")
    return m


def save_industries_msa():
    ret = process_dataset(industry4digit_msa)

    dpy = ret[('msa_id', 'industry_id', 'year')].reset_index()
    py = ret[('industry_id', 'year')][["complexity"]].reset_index()

    m = dpy.merge(py, on=["industry_id", "year"])
    m = merge_classification_by_id(m, location_classification,
                                   "msa_id", "location")
    m = merge_classification_by_id(m, industry_classification,
                                   "industry_id", "industry")
    return m


def save_industries_municipality():
    ret = process_dataset(industry4digit_municipality)

    m = ret[('municipality_id', 'industry_id', 'year')].reset_index()

    m = merge_classification_by_id(m, location_classification,
                                   "municipality_id", "location")
    m = merge_classification_by_id(m, industry_classification,
                                   "industry_id", "industry")
    return m


def save_occupations():
    ret = process_dataset(occupation2digit_industry2digit)
    m = ret[('occupation_id', 'industry_id')].reset_index()

    m = merge_classification_by_id(m, occupation_classification,
                                   "occupation_id", "industry")
    m = merge_classification_by_id(m, industry_classification,
                                   "industry_id", "industry")
    return m


def save_demographic():
    ret = process_dataset(gdp_real_department)
    gdp_real_df = ret[('department_id', 'year')]

    ret = process_dataset(gdp_nominal_department)
    gdp_nominal_df = ret[('department_id', 'year')]

    gdp_df = gdp_real_df.join(gdp_nominal_df).reset_index()

    ret = process_dataset(population)
    pop_df = ret[('department_id', 'year')].reset_index()

    m = gdp_df.merge(pop_df, on=["department_id", "year"], how="outer")

    m = merge_classification_by_id(m, location_classification,
                                   "department_id", "location")
    return m.sort_values(by=["department_id", "year"]).reset_index(drop=True)


def downloads():
    path = os.path.join(os.path.dirname(__file__), "../downloads/")

    def save(df, name):
        return df.to_csv(os.path.join(path, name))

    save(save_products_department(), "products_department.csv")
    save(save_products_msa(), "products_msa.csv")
    save(save_products_muni(), "products_municipality.csv")

    save(save_industries_department(), "industries_department.csv")
    save(save_industries_msa(), "industries_msa.csv")
    save(save_industries_municipality(), "industries_municipality.csv")

    save(save_occupations(), "occupations.csv")
    save(save_demographic(), "demographic.csv")

if __name__ == "__main__":

    app = create_app()
    with app.app_context():
        downloads()

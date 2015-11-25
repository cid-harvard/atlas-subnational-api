from colombia import create_app
from dataset_tools import process_dataset, merge_classification_by_id
from datasets import (trade4digit_country, trade4digit_department,
                      trade4digit_msa, trade4digit_municipality,
                      industry4digit_country, industry4digit_department,
                      industry4digit_msa, industry4digit_municipality,
                      occupation2digit_industry2digit, gdp_nominal_department,
                      gdp_real_department, population,
                      trade4digit_rcpy_country, trade4digit_rcpy_department,
                      trade4digit_rcpy_msa, trade4digit_rcpy_municipality)

from datasets import (product_classification, industry_classification,
                      location_classification, occupation_classification)

import os

classifications = {
    "occupation_id": {
        "name": "occupation",
        "classification": occupation_classification
    },
    "location_id": {
        "name": "location",
        "classification": location_classification
    },
    "product_id": {
        "name": "product",
        "classification": product_classification
    },
    "industry_id": {
        "name": "industry",
        "classification": industry_classification
    },
}


def merge_classifications(df):
    """Look for columns named classificationname_id and merge the
    classification that column."""

    index_cols = [a.name for a in df.index.levels]
    df = df.reset_index()

    for col, settings in classifications.items():
        if col in df.columns:
            df = merge_classification_by_id(
                df, settings["classification"],
                col, settings["name"])

    return df.set_index(index_cols)


def save_products_country():
    ret = process_dataset(trade4digit_country)
    dpy = ret[('location_id', 'product_id', 'year')]
    return merge_classifications(dpy)


def save_products_department():
    ret = process_dataset(trade4digit_department)

    dpy = ret[('location_id', 'product_id', 'year')].reset_index()
    py = ret[('product_id', 'year')][["pci"]].reset_index()
    dy = ret[('location_id', 'year')][["eci"]].reset_index()

    m = dpy.merge(py, on=["product_id", "year"])
    m = m.merge(dy, on=["location_id", "year"])

    m = merge_classifications(m.set_index(['location_id', 'product_id', 'year']))
    return m


def save_products_msa():
    ret = process_dataset(trade4digit_msa)

    df = ret[('location_id', 'product_id', 'year')].reset_index()
    py = ret[('product_id', 'year')][["pci"]].reset_index()
    dy = ret[('location_id', 'year')][["eci"]].reset_index()

    df = df.merge(py, on=["product_id", "year"])
    df = df.merge(dy, on=["location_id", "year"])

    df = merge_classifications(df.set_index(['location_id', 'product_id', 'year']))
    return df


def save_products_muni():
    ret = process_dataset(trade4digit_municipality)

    df = ret[('location_id', 'product_id', 'year')]
    df = merge_classifications(df)
    return df


def save_industries_country():
    ret = process_dataset(industry4digit_country)

    dpy = ret[('location_id', 'industry_id', 'year')]
    return merge_classifications(dpy)


def save_industries_department():
    ret = process_dataset(industry4digit_department)

    dpy = ret[('location_id', 'industry_id', 'year')].reset_index()
    py = ret[('industry_id', 'year')][["complexity"]].reset_index()

    m = dpy.merge(py, on=["industry_id", "year"])
    m = merge_classifications(m.set_index(['location_id', 'industry_id', 'year']))
    return m


def save_industries_msa():
    ret = process_dataset(industry4digit_msa)

    dpy = ret[('location_id', 'industry_id', 'year')].reset_index()
    py = ret[('industry_id', 'year')][["complexity"]].reset_index()

    m = dpy.merge(py, on=["industry_id", "year"])
    m = merge_classifications(m.set_index(['location_id', 'industry_id', 'year']))
    return m


def save_industries_municipality():
    ret = process_dataset(industry4digit_municipality)

    m = ret[('location_id', 'industry_id', 'year')]

    m = merge_classifications(m)
    return m


def save_occupations():
    ret = process_dataset(occupation2digit_industry2digit)
    m = ret[('occupation_id', 'industry_id')]

    m = merge_classifications(m)
    return m


def save_demographic():
    ret = process_dataset(gdp_real_department)
    gdp_real_df = ret[('location_id', 'year')]

    ret = process_dataset(gdp_nominal_department)
    gdp_nominal_df = ret[('location_id', 'year')]

    gdp_df = gdp_real_df.join(gdp_nominal_df).reset_index()

    ret = process_dataset(population)
    pop_df = ret[('location_id', 'year')].reset_index()

    m = gdp_df.merge(pop_df, on=["location_id", "year"], how="outer")

    m = merge_classifications(m.set_index(['location_id', 'year']))
    return m


def save_rcpy_country():

    ret = process_dataset(trade4digit_rcpy_country)
    df = ret[("country_id", "location_id", "product_id", "year")]

    m = merge_classifications(df)
    return m


def save_rcpy_department():

    ret = process_dataset(trade4digit_rcpy_department)
    df = ret[("country_id", "location_id", "product_id", "year")]

    m = merge_classifications(df)
    return m


def save_rcpy_msa():

    ret = process_dataset(trade4digit_rcpy_msa)
    df = ret[("country_id", "location_id", "product_id", "year")]

    m = merge_classifications(df)
    return m


def save_rcpy_municipality():

    ret = process_dataset(trade4digit_rcpy_municipality)
    df = ret[("country_id", "location_id", "product_id", "year")]

    m = merge_classifications(df)
    return m


def downloads():
    path = os.path.join(os.path.dirname(__file__), "../downloads/")

    def save(df, name):
        return df.to_csv(
            os.path.join(path, name),
            float_format='%.2f',
            index=False
        )

    save(save_rcpy_country(), "products_rcpy_country.csv")
    save(save_rcpy_department(), "products_rcpy_department.csv")
    save(save_rcpy_msa(), "products_rcpy_msa.csv")
    save(save_rcpy_municipality(), "products_rcpy_municipality.csv")

    save(save_products_country(), "products_country.csv")
    save(save_products_department(), "products_department.csv")
    save(save_products_msa(), "products_msa.csv")
    save(save_products_muni(), "products_municipality.csv")

    save(save_industries_country(), "industries_country.csv")
    save(save_industries_department(), "industries_department.csv")
    save(save_industries_msa(), "industries_msa.csv")
    save(save_industries_municipality(), "industries_municipality.csv")

    save(save_occupations(), "occupations.csv")
    save(save_demographic(), "demographic.csv")

if __name__ == "__main__":

    app = create_app()
    with app.app_context():
        downloads()

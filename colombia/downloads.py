from colombia import create_app
from dataset_tools import process_dataset, merge_classification_by_id
from datasets import (trade4digit_country, trade4digit_department,
                      trade4digit_msa, trade4digit_municipality,
                      industry4digit_country, industry4digit_department,
                      industry4digit_msa, industry4digit_municipality,
                      occupation2digit_industry2digit, gdp_nominal_department,
                      gdp_real_department, population,
                      trade4digit_rcpy_country, trade4digit_rcpy_department,
                      trade4digit_rcpy_msa, trade4digit_rcpy_municipality,
                      agproduct_level3_country, agproduct_level3_department,
                      agproduct_level3_municipality, livestock_level1_country,
                      livestock_level1_department,
                      livestock_level1_municipality, land_use_level2_country,
                      land_use_level2_department, land_use_level2_municipality,
                      farmtype_level2_country, farmtype_level2_department,
                      farmtype_level2_municipality, farmsize_level1_country,
                      farmsize_level1_department, farmsize_level1_municipality,
                      )

from datasets import (product_classification, industry_classification,
                      location_classification, occupation_classification,
                      country_classification, livestock_classification,
                      agproduct_classification, land_use_classification,
                      farmtype_classification, farmsize_classification)

from unidecode import unidecode

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
    "country_id": {
        "name": "country",
        "classification": country_classification
    },
    "livestock_id": {
        "name": "livestock",
        "classification": livestock_classification
    },
    "agproduct_id": {
        "name": "agproduct",
        "classification": agproduct_classification
    },
    "land_use_id": {
        "name": "land_use",
        "classification": land_use_classification
    },
    "farmtype_id": {
        "name": "farmtype",
        "classification": farmtype_classification
    },
    "farmsize_id": {
        "name": "farmsize",
        "classification": farmsize_classification
    },
}


def merge_classifications(df):
    """Look for columns named classificationname_id and merge the
    classification that column."""

    index_cols = [a.name for a in df.index.levels]
    df = df.reset_index()

    for col, settings in classifications.items():
        if col in df.columns:
            if settings["name"] == "location":
                name_columns = ["name"]
            else:
                name_columns = ["name", "name_es"]
            df = merge_classification_by_id(
                df, settings["classification"], col,
                prefix=settings["name"],
                name_columns=name_columns)
            # Deburr names
            name_col = "{}_name".format(settings["name"])
            df[name_col] = df[name_col].map(unidecode)

    return df.set_index(index_cols)


def save(path, df, name, format="excel", include_from_index=["year"]):

    if include_from_index is not None:
        df = df.reset_index(level=include_from_index)

    if format == "excel":
        df\
            .to_excel(
                os.path.join(path, name) + ".xlsx",
                float_format='%.2f',
                index=False,
                engine="xlsxwriter",
            )
    elif format == "csv":
        df\
            .to_csv(
                os.path.join(path, name) + ".csv",
                float_format='%.2f',
                index=False,
                compression="gzip"
            )
    elif format == "txt":
        df\
            .to_csv(
                os.path.join(path, name) + ".txt",
                float_format='%.2f',
                index=False,
                compression="gzip"
            )
    else:
        raise ValueError("Download format must be excel, csv or txt.")

def region_product_year(ret):
    """Merge region product year, product year and region year variable
    datasets."""

    df = ret[('location_id', 'product_id', 'year')].reset_index()
    py = ret[('product_id', 'year')][["pci"]].reset_index()
    dy = ret[('location_id', 'year')][["eci", "coi"]].reset_index()

    df = df.merge(py, on=["product_id", "year"])
    df = df.merge(dy, on=["location_id", "year"])

    return df.set_index(['location_id', 'product_id', 'year'])


def save_products_country():
    ret = process_dataset(trade4digit_country)
    m = region_product_year(ret)
    m = merge_classifications(m)
    return m


def save_products_department():
    ret = process_dataset(trade4digit_department)
    m = region_product_year(ret)
    m = merge_classifications(m)
    return m


def save_products_msa():
    ret = process_dataset(trade4digit_msa)
    m = region_product_year(ret)
    m = merge_classifications(m)
    return m


def save_products_muni():
    ret = process_dataset(trade4digit_municipality)

    df = ret[('location_id', 'product_id', 'year')]
    df = merge_classifications(df)

    pci = process_dataset(trade4digit_country)[('product_id', 'year')][["pci"]].reset_index()
    df = df.reset_index().merge(pci, on=["product_id", "year"], how="outer")
    return df.set_index(['location_id', 'product_id', 'year'])


def save_industries_country():
    ret = process_dataset(industry4digit_country)

    dpy = ret[('location_id', 'industry_id', 'year')].reset_index()
    py = ret[('industry_id', 'year')][["complexity"]].reset_index()

    m = dpy.merge(py, on=["industry_id", "year"])
    m = merge_classifications(m.set_index(['location_id', 'industry_id', 'year']))
    return m


def save_industries_department():
    ret = process_dataset(industry4digit_department)

    dpy = ret[('location_id', 'industry_id', 'year')].reset_index()
    py = ret[('industry_id', 'year')][["complexity"]].reset_index()
    ly = ret[('location_id', 'year')][["industry_eci", "industry_coi"]].reset_index()

    m = dpy.merge(py, on=["industry_id", "year"])
    m = m.merge(ly, on=["location_id", "year"])
    m = merge_classifications(m.set_index(['location_id', 'industry_id', 'year']))
    return m


def save_industries_msa():
    ret = process_dataset(industry4digit_msa)

    dpy = ret[('location_id', 'industry_id', 'year')].reset_index()
    py = ret[('industry_id', 'year')][["complexity"]].reset_index()
    ly = ret[('location_id', 'year')][["industry_eci", "industry_coi"]].reset_index()

    m = dpy.merge(py, on=["industry_id", "year"])
    m = m.merge(ly, on=["location_id", "year"])
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
    m["year"] = 2014
    return m.set_index("year")


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


def save_rcpy(rcpy_dataset):
    ret = process_dataset(rcpy_dataset)
    df = ret[("country_id", "location_id", "product_id", "year")]

    df = merge_classifications(df)

    pci = process_dataset(trade4digit_country)[('product_id', 'year')][["pci"]].reset_index()
    df = df.reset_index().merge(pci, on=["product_id", "year"], how="outer")
    return df.set_index(['country_id', 'location_id', 'product_id', 'year'])

def save_rcpy_country():
    return save_rcpy(trade4digit_rcpy_country)


def save_rcpy_department():
    return save_rcpy(trade4digit_rcpy_department)


def save_rcpy_msa():
    return save_rcpy(trade4digit_rcpy_msa)


def save_rcpy_municipality():
    return save_rcpy(trade4digit_rcpy_municipality)


def save_rural(path, datasets, facet=None, prefix=""):

    results = {}

    for geolevel, dataset in datasets.items():
        ret = process_dataset(dataset)

        if facet is None:
            facet = list(ret.keys())[0]

        results[geolevel] = merge_classifications(ret[facet])

        save(path, results[geolevel],
             "{}{}".format(prefix, geolevel),
             format="excel",
             include_from_index=None,
             )

    return results


def save_classifications(output_dir):
    import pandas as pd
    writer = pd.ExcelWriter(
        os.path.join(output_dir, "classifications.xls"),
        engine='xlsxwriter'
    )

    for col, settings in classifications.items():
        name = settings["name"]
        classification = settings["classification"]
        classification.table.to_excel(writer, sheet_name=name)

    writer.save()


def downloads():
    path = os.path.join(os.path.dirname(__file__), "../downloads/")

    save_classifications(path)

    save_rural(
        path,
        {
            "country": agproduct_level3_country,
            "department": agproduct_level3_department,
            "municipality": agproduct_level3_municipality,
        },
        prefix="agproduct_",
    )

    save_rural(
        path,
        {
            "country": livestock_level1_country,
            "department": livestock_level1_department,
            "municipality": livestock_level1_municipality,
        },
        prefix="livestock_",
    )

    save_rural(
        path,
        {
            "country": land_use_level2_country,
            "department": land_use_level2_department,
            "municipality": land_use_level2_municipality,
        },
        prefix="land_use_",
    )

    save_rural(
        path,
        {
            "country": farmtype_level2_country,
            "department": farmtype_level2_department,
            "municipality": farmtype_level2_municipality,
        },
        prefix="farmtype_",
    )

    save_rural(
        path,
        {
            "country": farmsize_level1_country,
            "department": farmsize_level1_department,
            "municipality": farmsize_level1_municipality,
        },
        prefix="farmsize_",
    )

    save(path, save_rcpy_country(), "products_rcpy_country", format="csv")
    save(path, save_rcpy_department(), "products_rcpy_department", format="csv")
    save(path, save_rcpy_msa(), "products_rcpy_msa", format="csv")
    save(path, save_rcpy_municipality(), "products_rcpy_municipality", format="csv")

    save(path, save_products_country(), "products_country")
    save(path, save_products_department(), "products_department")
    save(path, save_products_msa(), "products_msa")
    save(path, save_products_muni(), "products_municipality", format="csv")

    save(path, save_industries_country(), "industries_country")
    save(path, save_industries_department(), "industries_department")
    save(path, save_industries_msa(), "industries_msa")
    save(path, save_industries_municipality(), "industries_municipality", format="txt")

    save(path, save_occupations(), "occupations")
    save(path, save_demographic(), "demographic")

if __name__ == "__main__":

    app = create_app()
    with app.app_context():
        downloads()

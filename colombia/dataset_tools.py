import pandas as pd
import numpy as np

from atlas_core.helpers.data_import import translate_columns


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


def fillin(df, entities):
    """STATA style "fillin", make sure all permutations of entities in the
    index are in the dataset."""
    df = df.set_index(entities)
    return df.reindex(
        pd.MultiIndex.from_product(df.index.levels, names=df.index.names))


def cut_columns(df, columns):
    return df[list(columns)]


# Classification.merge_to_table
# Classification.merge_index
def merge_to_table(classification, classification_name, df, merge_on):
    code_to_id = classification.reset_index()[["code", "index"]]
    code_to_id.columns = ["code", classification_name]
    code_to_id = code_to_id.set_index("code")
    return df.merge(code_to_id, left_on=merge_on,
                    right_index=True, how="left")


def process_dataset(dataset):

    # Read dataset and fix up columns
    df = dataset["read_function"]()
    df = translate_columns(df, dataset["field_mapping"])
    df = cut_columns(df, dataset["field_mapping"].values())

    if "hook_pre_merge" in dataset:
        df = dataset["hook_pre_merge"](df)

    # Zero-pad digits of n-digit codes
    for field, length in dataset["digit_padding"].items():
        df[field] = df[field].astype(int).astype(str).str.zfill(length)

    # Make sure the dataset is rectangularized by the facet fields
    df = fillin(df, dataset["facet_fields"]).reset_index()

    # Merge in IDs for entity codes
    for field_name, c in dataset["classification_fields"].items():
        classification_table = c["classification"].level(c["level"])
        df = merge_to_table(classification_table,
                            field_name + "_id",
                            df, field_name)

    # Gather each facet dataset (e.g. DY, PY, DPY variables from DPY dataset)
    facet_outputs = {}
    for facet_fields, aggregations in dataset["facets"].items():
        facet_groupby = df.groupby(facet_fields)

        # Do specified aggregations / groupings for each column
        # like mean, first, min, rank, etc
        agg_outputs = []
        for agg_field, agg_func in aggregations.items():
            agged_row = agg_func(facet_groupby[[agg_field]])
            agg_outputs.append(agged_row)

        facet = pd.concat(agg_outputs, axis=1)
        facet_outputs[facet_fields] = facet

    return facet_outputs

# Cleaning notes
# ==============
# [OK] Fix column names
# [OK] Cut columns
# Check / Fix types

# [] Prefiltering if needed

# [OK] Fill digit numbers on classification fields if necessary
# [OK] Rectangularize by facet fields? If this comes from classification, do this later
# [OK] Merge classification fields, convert from code to ID

# [OK] Group by entities to get facets
# [OK] Aggregate each facets
# [OK] - eci / pci first()
# [OK] - export_value sum()
# []   - generate rank fields rank(method='dense')??
# [] Filtrations on facets???
# [OK] Returns a dict of facet -> dataframe indexed by facet keys

# [] Merge similar facet data (DY datasets together, etc)
# [] Function to generate other cross-dataset columns: gdp per capita

# [] Save merged facets into hdf5 file
# [] Load merged facet to given model


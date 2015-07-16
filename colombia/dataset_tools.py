import pandas as pd
import numpy as np

from atlas_core.helpers.data_import import translate_columns
from reckoner import assertions

from clint.textui import puts, indent, colored
from io import StringIO


def classification_to_models(classification, model):
    models = []
    for index, row in classification.table.iterrows():
        row = row.replace([np.nan], [None])
        m = model()
        m.id = index.item()
        m.code = row["code"]
        m.name_en = row["name"]
        if "name_es" in row:
            m.name_es = row["name_es"]
        if "name_short_en" in row:
            m.name_short_en = row["name_short_en"]
        if "name_short_es" in row:
            m.name_short_es = row["name_short_es"]
        m.level = row["level"]
        m.parent_id = row["parent_id"]
        models.append(m)
    return models


def fillin(df, entities):
    """STATA style 'fillin', makes sure all combinations of entities in the
    index are in the dataset."""
    df = df.set_index(entities)
    return df.reindex(
        pd.MultiIndex.from_product(df.index.levels, names=df.index.names))


def cut_columns(df, columns):
    return df[list(columns)]


# Classification.merge_to_table
# Classification.merge_index
def merge_to_table(classification, classification_name, df, merge_on):
    """Merge a classification to a table, given the code field."""
    code_to_id = classification.reset_index()[["code", "index"]]
    code_to_id.columns = ["code", classification_name]
    code_to_id = code_to_id.set_index("code")
    return df.merge(code_to_id, left_on=merge_on,
                    right_index=True, how="left")


def good(msg):
    return puts("[^‿^] " + colored.green(msg))


def warn(msg):
    return puts("[ಠ_ಠ] " + colored.yellow(msg))


def bad(msg):
    return puts("[ಠ益ಠ] " + colored.red(msg))

indented = lambda: indent(4, quote=colored.cyan("> "))


def process_dataset(dataset):

    good("Processing a new dataset!")

    # Read dataset and fix up columns
    df = dataset["read_function"]()
    df = translate_columns(df, dataset["field_mapping"])
    df = cut_columns(df, dataset["field_mapping"].values())

    if "hook_pre_merge" in dataset:
        df = dataset["hook_pre_merge"](df)

    puts("Dataset overview:")
    with indented():
        infostr = StringIO()
        df.info(buf=infostr, memory_usage=True, null_counts=True)
        puts(infostr.getvalue())

    for field in dataset["facet_fields"]:
        try:
            assertions.assert_none_missing(df[field])
        except AssertionError:
            warn("Field '{}' has {} missing values."
                 .format(field, df[field].isnull().sum()))

    # Zero-pad digits of n-digit codes
    for field, length in dataset["digit_padding"].items():
        try:
            assertions.assert_is_zeropadded_string(df[field])
        except AssertionError:
            warn("Field '{}' is not padded to {} digits."
                 .format(field, length))
            df[field] = df[field].astype(int).astype(str).str.zfill(length)

    # Make sure the dataset is rectangularized by the facet fields
    try:
        assertions.assert_rectangularized(df, dataset["facet_fields"])
    except AssertionError:
        warn("Dataset is not rectangularized on fields {}"
             .format(dataset["facet_fields"]))
        df = fillin(df, dataset["facet_fields"]).reset_index()

    # Merge in IDs for entity codes
    for field_name, c in dataset["classification_fields"].items():
        classification_table = c["classification"].level(c["level"])

        (p_nonmatch_rows, p_nonmatch_unique,
         codes_missing, codes_unused) = assertions.matching_stats(df[field_name], classification_table)

        if p_nonmatch_rows > 0:
            puts("When Merging field {}:".format(field_name))
            with indented():
                bad("Percentage of nonmatching rows: {}".format(p_nonmatch_rows))
                bad("Percentage of nonmatching codes: {}".format(p_nonmatch_unique))
                bad("Codes missing in classification: {}".format(codes_missing))
                bad("Codes unused: {}".format(codes_unused))

        df = merge_to_table(classification_table,
                            field_name + "_id",
                            df, field_name)

    # Gather each facet dataset (e.g. DY, PY, DPY variables from DPY dataset)
    facet_outputs = {}
    for facet_fields, aggregations in dataset["facets"].items():
        puts("Working on facet: {}".format(facet_fields))
        facet_groupby = df.groupby(facet_fields)

        # Do specified aggregations / groupings for each column
        # like mean, first, min, rank, etc
        agg_outputs = []
        for agg_field, agg_func in aggregations.items():
            with indented():
                puts("Working on: {}".format(agg_field))

            agged_row = agg_func(facet_groupby[[agg_field]])
            agg_outputs.append(agged_row)

        facet = pd.concat(agg_outputs, axis=1)
        facet_outputs[facet_fields] = facet

    puts("Done! ヽ(◔◡◔)ﾉ")

    return facet_outputs

# Cleaning notes
# ==============
# [] Merge similar facet data (DY datasets together, etc)
# [] Function to generate other cross-dataset columns: gdp per capita

# [] Save merged facets into hdf5 file
# [] Load merged facet to given model


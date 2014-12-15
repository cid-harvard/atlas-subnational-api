import pandas as pd
import numpy as np

from colombia import models


def make_cpy(line):
    dpy = models.DepartmentProductYear()
    dpy.export_value = line["export_value"]
    dpy.rca = line["rca"]
    dpy.density = line["density"]
    dpy.cog = line["cog"]
    dpy.coi = line["coi"]
    dpy.year = line["year"]
    return dpy

def make_cy(line):
    dpy = models.DepartmentProductYear()
    dpy.export_value = line["export_value"]
    dpy.rca = line["rca"]
    dpy.density = line["density"]
    dpy.cog = line["cog"]
    dpy.coi = line["coi"]
    return dpy


def process_cpy(cpy):
    """Take a dataframe and return

    """

    cpy_out = cpy.apply(make_cpy, axis=1)

    cy_out = cpy.apply(make_cy, axis=1)

    return [cy_out, [1]*4, cpy_out]


def translate_columns(df, translation_table):
    """Take a dataframe, filter only the columns we want, rename them, drop all
    other columns.

    :param df: pandas dataframe
    :param translation_table: dict[column_name_before -> column_name_after]
    """
    return df[list(translation_table.keys())]\
        .rename(columns=translation_table)


# Taken from ecomplexity_from_cepii_xx_dollar.dta
# Sample: [u'department', u'hs4', u'peso', u'dollar', u'__000001', u'M',
# u'density', u'eci', u'pci', u'diversity', u'ubiquity', u'coi', u'cog',
# u'rca']
aduanas_to_atlas = {
    "department": "department",
    "hs4": "product",
    "year": None,
    "value": "export_value",
    "density": "density",
    "eci": "eci",
    "pci": "pci",
    "diversity": "diversity",
    "ubiquity": "ubiquity",
    "coi": "coi",
    "cog": "cog",
    "rca": "rca"
}


import unittest


class ImporterTestCase(unittest.TestCase):

    def test_translate_columns(self):

        randomdata = pd.DataFrame(np.random.randn(6, 4),
                                  columns=list('abcd'))

        translation_table = {"a": "x", "b": "y", "c": "z"}
        translated = translate_columns(
            randomdata,
            translation_table
        )

        self.assertEquals(3, len(translated.columns))
        self.assertIn("x", translated.columns)
        self.assertIn("y", translated.columns)
        self.assertIn("z", translated.columns)
        self.assertNotIn("a", translated.columns)
        self.assertNotIn("b", translated.columns)
        self.assertNotIn("c", translated.columns)
        self.assertNotIn("d", translated.columns)

    def test_process_cpy(self):

        # Pass in a CPY and an ecomplexity file
        # Get import / export from CPY
        # Get CY / PY from ecomplexity

        #
        data = [
            {"department": 10, "product": 22, "year": 1998, "export_value": 1234,
             "density": 1, "eci": 4, "pci": 3, "diversity": 1, "ubiquity": 1,
             "coi": 1, "cog": 1, "rca": 1},
            {"department": 10, "product": 24, "year": 1998, "export_value": 4321,
             "density": 1, "eci": 4, "pci": 1, "diversity": 1, "ubiquity": 1,
             "coi": 1, "cog": 1, "rca": 1},
            {"department": 10, "product": 22, "year": 1999, "export_value": 9999,
             "density": 1, "eci": 7, "pci": 3, "diversity": 1, "ubiquity": 1,
             "coi": 1, "cog": 1, "rca": 1},
        ]
        data = pd.DataFrame.from_dict(data)

        # CPY
        cy, py, cpy = process_cpy(data)
        len(py) == 3  # product, year, pci, pci_rank

        # TODO imports
        # TODO distance vs density

        len(cpy) == 3  # cpy: export, rca, density, cog, coi
        self.assertEquals(cpy[0].export_value, 1234)
        self.assertEquals(cpy[0].rca, 1)
        self.assertEquals(cpy[0].density, 1)
        self.assertEquals(cpy[0].cog, 1)
        self.assertEquals(cpy[0].coi, 1)
        self.assertEquals(cpy[0].year, 1998)
        self.assertEquals(cpy[1].export_value, 4321)
        self.assertEquals(cpy[1].rca, 1)
        self.assertEquals(cpy[1].density, 1)
        self.assertEquals(cpy[1].cog, 1)
        self.assertEquals(cpy[1].coi, 1)
        self.assertEquals(cpy[1].year, 1998)
        self.assertEquals(cpy[2].export_value, 9999)
        self.assertEquals(cpy[2].rca, 1)
        self.assertEquals(cpy[2].density, 1)
        self.assertEquals(cpy[2].cog, 1)
        self.assertEquals(cpy[2].coi, 1)
        self.assertEquals(cpy[2].year, 1999)


        #len(cy) == 2  # department, year, eci, eci_rank, diversity
        #self.assertEquals(cy[0].department, 9999)
        #self.assertEquals(cy[0].eci, 1)
        #self.assertEquals(cy[0].eci_rank, 1)
        #self.assertEquals(cy[0].diversity, 1)

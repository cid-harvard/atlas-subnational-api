# vim: set fileencoding=utf8

import pandas as pd
import numpy as np

from io import StringIO

from atlas_core.helpers.data_import import translate_columns
from colombia import models, create_app
from colombia.core import db
from tests import BaseTestCase


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


def make_cpy(department_map, product_map):
    def inner(line):
        dpy = models.DepartmentProductYear()
        department = department_map[line["department"]]
        dpy.department = department
        product = product_map[line["product"]]
        dpy.product = product
        dpy.import_value = line["import_value"]
        dpy.export_value = line["export_value"]
        dpy.export_rca = line["export_rca"]
        dpy.density = line["density"]
        dpy.cog = line["cog"]
        dpy.coi = line["coi"]
        dpy.year = line["year"]
        return dpy
    return inner


def make_cy(department_map):
    def inner(line):
        dy = models.DepartmentYear()
        department = department_map[line["department"]]
        dy.department = department
        dy.year = line["year"]
        dy.eci = line["eci"]
        dy.diversity = line["diversity"]
        return dy
    return inner


def make_py(product_map):
    def inner(line):
        py = models.ProductYear()
        product = product_map[line["product"]]
        py.product = product
        py.year = line["year"]
        py.pci = line["pci"]
        return py
    return inner


def process_cpy(cpy, product_map, department_map):
    """Take a dataframe and return

    """

    cpy_out = cpy.apply(make_cpy(department_map, product_map), axis=1)

    cy = cpy.groupby(["department", "year"]).first().reset_index()
    cy_out = cy.apply(make_cy(department_map), axis=1)

    py = cpy.groupby(["product", "year"]).first().reset_index()
    py_out = py.apply(make_py(product_map), axis=1)

    return [cy_out, py_out, cpy_out]


# Taken from ecomplexity_from_cepii_xx_dollar.dta
# Sample: [u'department', u'hs4', u'peso', u'dollar', u'__000001', u'M',
# u'density', u'eci', u'pci', u'diversity', u'ubiquity', u'coi', u'cog',
# u'rca']
aduanas_to_atlas = {
    "department": "department",
    "hs4": "product",
    "year": "year",
    "dollar": "export_value",
    "density": "density",
    "eci": "eci",
    "pci": "pci",
    "diversity": "diversity",
    "ubiquity": "ubiquity",
    "coi": "coi",
    "cog": "cog",
    "rca": "export_rca"
}
aduanas_to_atlas_import = {
    "department": "department",
    "hs4": "product",
    "dollar": "import_value",
}


class ImporterTestCase(BaseTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

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


        department_map = {
            "10": models.Department(code="10", name="foo"),
        }

        product_map = {
            "22": models.HSProduct(code="22", name="Cars"),
            "24": models.HSProduct(code="24", name="Cars"),
        }

        db.session.add_all(department_map.values())
        db.session.add_all(product_map.values())
        db.session.commit()

        data = [
            {"department": "10", "product": "22", "year": 1998, "export_value": 1234,
             "density": 1, "eci": 4, "pci": 3, "diversity": 1, "ubiquity": 1,
             "coi": 1, "cog": 1, "export_rca": 1, "import_value": 22},
            {"department": "10", "product": "24", "year": 1998, "export_value": 4321,
             "density": 1, "eci": 4, "pci": 1, "diversity": 1, "ubiquity": 1,
             "coi": 1, "cog": 1, "export_rca": 1, "import_value": 44},
            {"department": "10", "product": "22", "year": 1999, "export_value": 9999,
             "density": 1, "eci": 7, "pci": 3, "diversity": 1, "ubiquity": 1,
             "coi": 1, "cog": 1, "export_rca": 1, "import_value": 666},
        ]
        data = pd.DataFrame.from_dict(data)

        # CPY
        cy, py, cpy = process_cpy(data, product_map, department_map)

        db.session.add_all(cy)
        db.session.add_all(py)
        db.session.add_all(cpy)
        db.session.commit()

        # TODO imports
        # TODO distance vs density

        len(cpy) == 3  # cpy: export, rca, density, cog, coi
        self.assertEquals(cpy[0].export_value, 1234)
        self.assertEquals(cpy[0].export_rca, 1)
        self.assertEquals(cpy[0].density, 1)
        self.assertEquals(cpy[0].cog, 1)
        self.assertEquals(cpy[0].coi, 1)
        self.assertEquals(cpy[0].year, 1998)
        self.assertEquals(cpy[0].department, department_map["10"])
        self.assertEquals(cpy[0].product, product_map["22"])
        self.assertEquals(cpy[1].export_value, 4321)
        self.assertEquals(cpy[1].import_value, 44)
        self.assertEquals(cpy[1].export_rca, 1)
        self.assertEquals(cpy[1].density, 1)
        self.assertEquals(cpy[1].cog, 1)
        self.assertEquals(cpy[1].coi, 1)
        self.assertEquals(cpy[1].department, department_map["10"])
        self.assertEquals(cpy[1].product, product_map["24"])
        self.assertEquals(cpy[1].year, 1998)
        self.assertEquals(cpy[2].export_value, 9999)
        self.assertEquals(cpy[2].export_rca, 1)
        self.assertEquals(cpy[2].density, 1)
        self.assertEquals(cpy[2].cog, 1)
        self.assertEquals(cpy[2].coi, 1)
        self.assertEquals(cpy[2].year, 1999)
        self.assertEquals(cpy[2].department, department_map["10"])
        self.assertEquals(cpy[2].product, product_map["22"])

        # TODO eci_rank

        len(cy) == 2  # department, year, eci, eci_rank, diversity
        self.assertNotEquals(cy[0].department_id, 10)
        self.assertEquals(cy[0].department, department_map["10"])
        self.assertEquals(cy[0].year, 1998)
        self.assertEquals(cy[0].eci, 4)
        self.assertEquals(cy[0].diversity, 1)
        self.assertNotEquals(cy[1].department_id, 10)
        self.assertEquals(cy[1].department, department_map["10"])
        self.assertEquals(cy[1].year, 1999)
        self.assertEquals(cy[1].eci, 7)
        self.assertEquals(cy[1].diversity, 1)

        # TODO pci_rank

        len(py) == 3  # product, year, pci, pci_rank
        self.assertNotEquals(py[0].product_id, 22)
        self.assertEquals(py[0].product, product_map["22"])
        self.assertEquals(py[0].year, 1998)
        self.assertEquals(py[0].pci, 3)
        self.assertNotEquals(py[1].product_id, 22)
        self.assertEquals(py[1].product, product_map["22"])
        self.assertEquals(py[1].year, 1999)
        self.assertEquals(py[1].pci, 3)
        self.assertNotEquals(py[2].product_id, 24)
        self.assertEquals(py[2].product, product_map["24"])
        self.assertEquals(py[2].year, 1998)
        self.assertEquals(py[2].pci, 1)



if __name__ == "__main__":

        app = create_app()

        with app.app_context():

            # Load products
            from linnaeus import classification
            product_classification = classification.load("product/HS/Atlas/out/hs92_atlas.csv")
            products = classification_to_models(product_classification,
                                                models.HSProduct)
            db.session.add_all(products)
            db.session.commit()

            product_map = {p.code: p for p in products}

            location_classification = classification.load("location/Colombia/DANE/out/locations_colombia_dane.csv")
            locations = classification_to_models(location_classification,
                                                models.Location)
            db.session.add_all(locations)
            db.session.commit()

            location_map = {l.code: l for l in locations}

            industry_classification = classification.load("industry/ISIC/Colombia/out/isic_ac_4.0.csv")
            industries = classification_to_models(industry_classification,
                                                  models.Industry)
            db.session.add_all(industries)
            db.session.commit()

            industry_map = {i.code: i for i in industries}


            dpy_file_template = "/Users/makmana/ciddata/Aduanas/ecomplexity_from_cepii_{0}_dollar.dta"
            dpy_import_file_template = "/Users/makmana/ciddata/Aduanas/ecomplexity_from_cepii_imp_{0}_dollar.dta"
            for i in range(8, 14):

                print(i)

                def parse_dpy(dpy_file, translation_table):
                    dpy = pd.read_stata(dpy_file)
                    dpy["year"] = 2000 + i

                    dpy = translate_columns(dpy, translation_table)
                    dpy = dpy[~dpy.department.isin([0, 1])]
                    dpy["product"] = dpy["product"].map(lambda x: str(int(x)).zfill(4)).astype(str)
                    dpy["department"] = dpy.department.map(lambda x: str(int(x)).zfill(2))
                    return dpy

                filename = dpy_file_template.format(str(i).zfill(2))
                dpy = parse_dpy(filename, aduanas_to_atlas)

                filename = dpy_import_file_template.format(str(i).zfill(2))
                imports_dpy = parse_dpy(filename, aduanas_to_atlas_import)
                imports_dpy = imports_dpy[["department", "product", "import_value"]]

                # Merge in imports with exports
                dpy = pd.merge(dpy,
                               imports_dpy,
                               on=["department", "product"], how="inner")

                cy, py, cpy = process_cpy(dpy, product_map, location_map)
                db.session.add_all(cy)
                db.session.add_all(py)
                db.session.add_all(cpy)
                db.session.commit()


            # Department - industry - year
            df = pd.read_stata("/Users/makmana/ciddata/PILA_andres/COL_PILA_ecomp-E_yir_2008-2012_rev3_dpto.dta")
            df = df[["year", "r", "i", "E_yir", "W_yir", "rca", "density", "cog", "coi"]]
            df = df[df.i != "."]

            df = df.merge(industry_classification.table, left_on="i",
                          right_on="code", how="inner")

            def make_diy():
                def inner(line):
                    dpy = models.DepartmentIndustryYear()
                    dpy.industry = industry_map[line["i"]]
                    dpy.department = location_map[line["r"]]
                    dpy.year = line["year"]
                    dpy.employment = line["E_yir"]
                    dpy.wages = line["W_yir"]

                    dpy.rca = line["rca"]
                    dpy.density = line["density"]
                    dpy.cog = line["cog"]
                    dpy.coi = line["coi"]

                    return dpy
                return inner
            cpy_out = df.apply(make_diy(), axis=1)
            db.session.add_all(cpy_out)
            db.session.commit()


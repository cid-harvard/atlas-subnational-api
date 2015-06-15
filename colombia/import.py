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


def process_department(dept):
    department_data = dept.groupby("department_code")\
        .first().reset_index()\
        [["department_name", "department_code"]]

    def make_department(line):
        d = models.Department()
        d.aggregation = "department"
        d.name = line["department_name"]
        d.code = line["department_code"]
        return d

    return department_data.apply(make_department, axis=1)


def process_product(prod):
    d = prod[["code", "name", "name_en", "name_es", "community_id"]]

    four_digit = d[d.code.str.len() == 4]
    two_digit = d[d.code.str.len() == 2]
    section = d[d.code.str.len() == 3]

    def product_maker(aggregation):
        def make_product(line):
            d = models.HSProduct()
            d.code = line["code"]
            d.name = line["name"]
            d.en = line["name_en"]
            d.es = line["name_es"]
            d.aggregation = aggregation
            if aggregation == "4digit":
                d.section_code = str(line["community_id"])
                community_index = section.code == d.section_code
                d.section_name = section[community_index].name_en.values[0]
                d.section_name_es = section[community_index].name_es.values[0]
            return d
        return make_product

    return (
        list(section.apply(product_maker("section"), axis=1)),
        list(two_digit.apply(product_maker("2digit"), axis=1)),
        list(four_digit.apply(product_maker("4digit"), axis=1))
    )


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

    def test_process_department(self):
        data = """
department_code	department_name	municipality_code	municipality_name	city	rural	midsize	pop_2012	nbi
08	Atlántico	08849	Usiacurí	FALSE	TRUE	FALSE	9238	43.27979231
11	"Bogotá, D.C."	11001	"Bogotá, D.C."	TRUE	FALSE	FALSE	7571345	9.20300877
13	Bolívar	13001	Cartagena	TRUE	FALSE	FALSE	967051	26.01059996"""

        data = pd.read_table(StringIO(data), encoding="utf-8",
                             dtype={"department_code": np.object})
        d = process_department(data)
        db.session.add_all(d)
        db.session.commit()

        # TODO population, gdp

        self.assertEquals(d[0].aggregation, "department")
        self.assertEquals(d[0].name, u"Atlántico")
        self.assertEquals(d[0].code, "08")

        self.assertEquals(d[1].aggregation, "department")
        self.assertEquals(d[1].name, u"Bogotá, D.C.")
        self.assertEquals(d[1].code, "11")

        self.assertEquals(d[2].aggregation, "department")
        self.assertEquals(d[2].name, u"Bolívar")
        self.assertEquals(d[2].code, "13")

    def test_process_product(self):

        data = """
code	name	name_en	name_es	community_id
0101	Live horses, asses, mules or hinnies	Horses	Caballos	106
0102	Live bovine animals	Bovines	Bovinos	116
106	Animal & Animal Products	Animal & Animal Products	NULL	106
116	Vegetable Products	Vegetable Products	NULL	116
04	Dairy, Eggs, Honey, & Ed. Products	Dairy, Honey, & Ed. Prod.	Productos lácteos, la miel, y Ed. Prod.	106
06	Live Trees & Other Plants	 Trees & Plants	 Árboles y Plantas	116"""

        data = pd.read_table(StringIO(data), encoding="utf-8",
                             dtype={"code": np.object})
        section, two_digit, four_digit = process_product(data)

        db.session.add_all(section)
        db.session.add_all(two_digit)
        db.session.add_all(four_digit)
        db.session.commit()

        len(four_digit) == 2
        self.assertEquals(four_digit[0].name, "Live horses, asses, mules or hinnies")
        self.assertEquals(four_digit[0].en, "Horses")
        self.assertEquals(four_digit[0].es, "Caballos")
        self.assertEquals(four_digit[0].code, "0101")
        self.assertEquals(four_digit[0].aggregation, "4digit")
        self.assertEquals(four_digit[0].section_code, "106")
        self.assertEquals(four_digit[0].section_name, "Animal & Animal Products")
        self.assertEquals(four_digit[1].name, "Live bovine animals")
        self.assertEquals(four_digit[1].en, "Bovines")
        self.assertEquals(four_digit[1].es, "Bovinos")
        self.assertEquals(four_digit[1].code, "0102")
        self.assertEquals(four_digit[1].aggregation, "4digit")
        self.assertEquals(four_digit[1].section_code, "116")
        self.assertEquals(four_digit[1].section_name, "Vegetable Products")

        len(two_digit) == 2
        self.assertEquals(two_digit[0].name, "Dairy, Eggs, Honey, & Ed. Products")
        self.assertEquals(two_digit[0].en, "Dairy, Honey, & Ed. Prod.")
        self.assertEquals(two_digit[0].es, "Productos lácteos, la miel, y Ed. Prod.")
        self.assertEquals(two_digit[0].code, "04")
        self.assertEquals(two_digit[0].aggregation, "2digit")
        self.assertEquals(two_digit[0].section_code, None)
        self.assertEquals(two_digit[0].section_name, None)
        self.assertEquals(two_digit[1].name, "Live Trees & Other Plants")
        self.assertEquals(two_digit[1].en, " Trees & Plants")
        self.assertEquals(two_digit[1].es, " Árboles y Plantas")
        self.assertEquals(two_digit[1].code, "06")
        self.assertEquals(two_digit[1].aggregation, "2digit")
        self.assertEquals(two_digit[1].section_code, None)
        self.assertEquals(two_digit[1].section_name, None)

        len(section) == 2
        self.assertEquals(section[0].name, "Animal & Animal Products")
        self.assertEquals(section[0].en, "Animal & Animal Products")
        self.assertEquals(section[0].es, None)
        self.assertEquals(section[0].code, "106")
        self.assertEquals(section[0].aggregation, "section")
        self.assertEquals(section[0].section_code, None)
        self.assertEquals(section[0].section_name, None)
        self.assertEquals(section[1].name, "Vegetable Products")
        self.assertEquals(section[1].en, "Vegetable Products")
        self.assertEquals(section[1].es, None)
        self.assertEquals(section[1].code, "116")
        self.assertEquals(section[1].aggregation, "section")
        self.assertEquals(section[1].section_code, None)
        self.assertEquals(section[1].section_name, None)


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


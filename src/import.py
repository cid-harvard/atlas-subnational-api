# vim: set fileencoding=utf8

import pandas as pd
import numpy as np

from io import StringIO

from colombia import models, create_app
from colombia.models import db
from tests import ChassisTestCase


def make_cpy(department_map, product_map):
    def inner(line):
        dpy = models.DepartmentProductYear()
        department = department_map[line["department"]]
        dpy.department = department
        product = product_map[line["product"]]
        dpy.product = product
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
    d = prod[["Code", "hs4_name_en"]]

    four_digit = d[d.Code.str.len() == 4]
    two_digit = d[d.Code.str.len() == 2]
    section = d[d.Code.str.len() == 3]


    def product_maker(aggregation):
        def make_product(line):
            d = models.HSProduct()
            d.code = line["Code"]
            d.name = line["hs4_name_en"]
            d.aggregation = aggregation
            return d
        return make_product

    return (
        list(section.apply(product_maker("section"), axis=1)),
        list(two_digit.apply(product_maker("2digit"), axis=1)),
        list(four_digit.apply(product_maker("4digit"), axis=1))
    )


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
    "year": "year",
    "dollar": "export_value",
    "density": "density",
    "eci": "eci",
    "pci": "pci",
    "diversity": "diversity",
    "ubiquity": "ubiquity",
    "coi": "coi",
    "cog": "cog",
    "rca": "rca"
}


class ImporterTestCase(ChassisTestCase):

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
             "coi": 1, "cog": 1, "export_rca": 1},
            {"department": "10", "product": "24", "year": 1998, "export_value": 4321,
             "density": 1, "eci": 4, "pci": 1, "diversity": 1, "ubiquity": 1,
             "coi": 1, "cog": 1, "export_rca": 1},
            {"department": "10", "product": "22", "year": 1999, "export_value": 9999,
             "density": 1, "eci": 7, "pci": 3, "diversity": 1, "ubiquity": 1,
             "coi": 1, "cog": 1, "export_rca": 1},
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
Code	hs4_name	hs4_name_en	community
0101	Live horses, asses, mules or hinnies	Horses	106
0102	Live bovine animals	Bovines	106
106	Animal & Animal Products	Animal & Animal Products	106
116	Vegetable Products	Vegetable Products	116
04	Dairy, Eggs, Honey, & Ed. Products	Dairy, Honey, & Ed. Prod.	106
06	Live Trees & Other Plants	Trees & Plants	116"""

        data = pd.read_table(StringIO(data), encoding="utf-8",
                             dtype={"Code": np.object})
        section, two_digit, four_digit = process_product(data)

        db.session.add_all(section)
        db.session.add_all(two_digit)
        db.session.add_all(four_digit)
        db.session.commit()

        len(four_digit) == 2
        self.assertEquals(four_digit[0].name, "Horses")
        self.assertEquals(four_digit[0].code, "0101")
        self.assertEquals(four_digit[0].aggregation, "4digit")
        self.assertEquals(four_digit[1].name, "Bovines")
        self.assertEquals(four_digit[1].code, "0102")
        self.assertEquals(four_digit[1].aggregation, "4digit")

        len(two_digit) == 2
        self.assertEquals(two_digit[0].name, "Dairy, Honey, & Ed. Prod.")
        self.assertEquals(two_digit[0].code, "04")
        self.assertEquals(two_digit[0].aggregation, "2digit")
        self.assertEquals(two_digit[1].name, "Trees & Plants")
        self.assertEquals(two_digit[1].code, "06")
        self.assertEquals(two_digit[1].aggregation, "2digit")

        len(section) == 2
        self.assertEquals(section[0].name, "Animal & Animal Products")
        self.assertEquals(section[0].code, "106")
        self.assertEquals(section[0].aggregation, "section")
        self.assertEquals(section[1].name, "Vegetable Products")
        self.assertEquals(section[1].code, "116")
        self.assertEquals(section[1].aggregation, "section")


if __name__ == "__main__":

        app = create_app()

        with app.app_context():
            departments_file = "/Users/makmana/ciddata/metadata_data/location_table_with_pop.txt"
            products_file = "/Users/makmana/ciddata/metadata_data/hs4_all.tsv"

            # Load departments
            departments = pd.read_table(departments_file, encoding="utf-16",
                                        dtype={"department_code": np.object})
            departments = process_department(departments)
            db.session.add_all(departments)
            db.session.commit()

            department_map = {d.code: d for d in departments}


            # Load products
            products = pd.read_table(products_file, encoding="utf-8",
                                 dtype={"Code": np.object})
            section, two_digit, four_digit = process_product(products)
            db.session.add_all(section)
            db.session.add_all(two_digit)
            db.session.add_all(four_digit)
            db.session.commit()

            product_map = {p.code: p for p in section + two_digit + four_digit}

            dpy_file = "/Users/makmana/ciddata/Aduanas/ecomplexity_from_cepii_08_dollar.dta"
            dpy = pd.read_stata(dpy_file)
            dpy = dpy[~dpy.department.isin([0, 1])]
            dpy["year"] = 2008
            dpy["hs4"] = dpy.hs4.map(lambda x: str(int(x)).zfill(4)).astype(str)
            dpy["department"] = dpy.department.map(lambda x: str(x).zfill(2))
            dpy = translate_columns(dpy, aduanas_to_atlas)

            cy, py, cpy = process_cpy(dpy, product_map, department_map)
            db.session.add_all(cy)
            db.session.add_all(py)
            db.session.add_all(cpy)
            db.session.commit()
            import ipdb; ipdb.set_trace()

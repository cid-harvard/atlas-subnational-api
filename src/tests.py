from flask.ext.testing import TestCase

from colombia import create_app
from colombia import ext
from colombia.models import Municipality, HSProduct, DepartmentProductYear
from colombia.views import (HSProductAPI, HSProductListAPI, DepartmentAPI,
                            DepartmentListAPI,
                            DepartmentProductYearByDepartmentAPI,
                            DepartmentProductYearByProductAPI)

import factories

db = ext.db
api = ext.api


class ChassisTestCase(TestCase):

    """Base TestCase to add in convenience functions, defaults and custom
    asserts."""

    def create_app(self):
        return create_app({"SQLALCHEMY_DATABASE_URI":
                           self.SQLALCHEMY_DATABASE_URI,
                           "TESTING": True})

    def setUp(self):
        db.create_all()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        ext.reset()


class TestModels(ChassisTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def try_model(self, factory, model):
        self.assertEquals(db.session.query(model).count(), 0)

        thing = factory()
        db.session.commit()

        self.assertEquals(db.session.query(model).count(), 1)

    def test_locations(self):
        self.try_model(factories.Municipality, Municipality)

    def test_products(self):
        self.try_model(factories.HSProduct, HSProduct)

    def test_DepartmentProductYear(self):
        self.try_model(factories.DepartmentProductYear, DepartmentProductYear)


class TestMetadataAPIs(ChassisTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def test_get_hsproduct(self):

        product = factories.HSProduct()
        db.session.commit()

        response = self.client.get(api.url_for(HSProductAPI,
                                               code=product.code))
        self.assert_200(response)
        self.assertEquals(response.json["code"], product.code)
        self.assertEquals(response.json["name"], product.name)

    def test_get_hsproducts(self):

        p1 = factories.HSProduct(aggregation="2digit", code="22")
        p2 = factories.HSProduct(aggregation="4digit", code="1234")
        p3 = factories.HSProduct(aggregation="section", code="A")
        db.session.commit()

        for p in [p1, p2, p3]:
            response = self.client.get(api.url_for(HSProductListAPI,
                                                   aggregation=p.aggregation))
            self.assert_200(response)
            self.assertEquals(len(response.json), 1)
            self.assertEquals(response.json[str(p.id)]["code"], p.code)

        response = self.client.get(api.url_for(HSProductListAPI))
        self.assert_200(response)
        self.assertEquals(len(response.json), 3)

    def test_get_department(self):

        dept = factories.Department()
        db.session.commit()

        response = self.client.get(api.url_for(DepartmentAPI,
                                               code=dept.code))
        self.assert_200(response)
        self.assertEquals(response.json["code"], dept.code)
        self.assertEquals(response.json["name"], dept.name)
        self.assertEquals(response.json["population"], dept.population)
        self.assertEquals(response.json["gdp"], dept.gdp)

    def test_get_departments(self):

        factories.Department(code="22")
        factories.Department(code="24")
        factories.Department(code="26")
        db.session.commit()

        response = self.client.get(api.url_for(DepartmentListAPI))
        self.assert_200(response)
        self.assertEquals(len(response.json), 3)
        self.assertEquals(set(v["code"] for k, v in response.json.items()),
                          set(["22", "24", "26"]))

    def test_get_department_product_year_by_department(self):

        a = factories.DepartmentProductYear(year=2012)
        b = factories.DepartmentProductYear(year=2012, department=a.department)
        c = factories.DepartmentProductYear(year=2012, department=a.department)
        entries = [a, b, c]
        db.session.commit()

        response = self.client.get(
            api.url_for(DepartmentProductYearByDepartmentAPI,
                        department=a.department_id,
                        year=2012))
        self.assert_200(response)
        self.assertEquals(len(response.json), 3)
        for result in response.json:
            self.assertIn(result["import_value"],
                          [x.import_value for x in entries])
            self.assertIn(result["export_value"],
                          [x.export_value for x in entries])
            self.assertIn(result["export_rca"],
                          [x.export_rca for x in entries])
            self.assertIn(result["distance"],
                          [x.distance for x in entries])
            self.assertIn(result["cog"],
                          [x.cog for x in entries])
            self.assertIn(result["coi"],
                          [x.coi for x in entries])

        # Add a 2010 datapoint
        factories.DepartmentProductYear(year=2010, department=a.department)
        db.session.commit()

        # Should get it when we query for all years
        response = self.client.get(
            "/trade/departments/{0}/".format(a.department_id))
        self.assertEquals(len(response.json), 4)

        # But not when we query for 2012
        response = self.client.get(
            api.url_for(DepartmentProductYearByDepartmentAPI,
                        department=a.department_id,
                        year=2012))
        self.assertEquals(len(response.json), 3)


    def test_get_department_product_year_by_product(self):

        a = factories.DepartmentProductYear(year=2012)
        b = factories.DepartmentProductYear(year=2012, product=a.product)
        c = factories.DepartmentProductYear(year=2012, product=a.product)
        entries = [a, b, c]
        db.session.commit()

        response = self.client.get(
            api.url_for(DepartmentProductYearByProductAPI,
                        product=a.product_id,
                        year=2012))
        self.assert_200(response)
        self.assertEquals(len(response.json), 3)
        for result in response.json:
            self.assertIn(result["import_value"],
                          [x.import_value for x in entries])
            self.assertIn(result["export_value"],
                          [x.export_value for x in entries])
            self.assertIn(result["export_rca"],
                          [x.export_rca for x in entries])
            self.assertIn(result["distance"],
                          [x.distance for x in entries])
            self.assertIn(result["cog"],
                          [x.cog for x in entries])
            self.assertIn(result["coi"],
                          [x.coi for x in entries])

        # Add a 2010 datapoint
        factories.DepartmentProductYear(year=2010, product=a.product)
        db.session.commit()

        # Should get it when we query for all years
        response = self.client.get(
            "/trade/products/{0}/".format(a.product_id))
        self.assertEquals(len(response.json), 4)

        # But not when we query for 2012
        response = self.client.get(
            api.url_for(DepartmentProductYearByProductAPI,
                        product=a.product_id,
                        year=2012))
        self.assertEquals(len(response.json), 3)

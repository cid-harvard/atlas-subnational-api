from flask import url_for

from colombia import factories
from colombia.core import db
from colombia.tests import ChassisTestCase


class TestDataAPIs(ChassisTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def test_get_department_product_year_by_department(self):

        a = factories.DepartmentProductYear(year=2012)
        b = factories.DepartmentProductYear(year=2012, department=a.department)
        c = factories.DepartmentProductYear(year=2012, department=a.department)
        entries = [a, b, c]
        db.session.commit()

        response = self.client.get(
            url_for("products.department_product_year",
                    department=a.department_id,
                    year=2012))
        self.assert_200(response)
        response_data = response.json["data"]
        self.assertEquals(len(response_data), 3)
        for result in response_data:
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
            url_for("products.department_product_year",
                    department=a.department_id))
        self.assertEquals(len(response.json["data"]), 4)

        # But not when we query for 2012
        response = self.client.get(
            url_for("products.department_product_year",
                    department=a.department_id,
                    year=2012))
        self.assertEquals(len(response.json["data"]), 3)

    def test_get_department_product_year_by_product(self):

        a = factories.DepartmentProductYear(year=2012)
        b = factories.DepartmentProductYear(year=2012, product=a.product)
        c = factories.DepartmentProductYear(year=2012, product=a.product)
        entries = [a, b, c]
        db.session.commit()

        response = self.client.get(
            url_for("products.department_product_year_by_product",
                        product=a.product_id,
                        year=2012))
        self.assert_200(response)
        response_data = response.json["data"]
        self.assertEquals(len(response_data), 3)
        for result in response_data:
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
            url_for("products.department_product_year_by_product",
                    product=a.product_id))
        self.assertEquals(len(response.json["data"]), 4)

        # But not when we query for 2012
        response = self.client.get(
            url_for("products.department_product_year_by_product",
                    product=a.product_id,
                    year=2012))
        self.assertEquals(len(response.json["data"]), 3)

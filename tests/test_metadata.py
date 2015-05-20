from flask import url_for

from colombia import factories
from colombia.core import db
from colombia.tests import ChassisTestCase


class TestMetadataAPIs(ChassisTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def test_get_hsproduct(self):

        product = factories.HSProduct()
        db.session.commit()

        response = self.client.get(url_for("metadata.product",
                                           product_id=product.id))
        self.assert_200(response)
        response_json = response.json["data"]
        self.assertEquals(response_json["code"], product.code)
        self.assertEquals(response_json["name"], product.name)

    def test_get_hsproducts(self):

        p1 = factories.HSProduct(aggregation="2digit", code="22")
        p2 = factories.HSProduct(aggregation="4digit", code="6208",
                                 section_code="416", section_name="Textiles")
        p3 = factories.HSProduct(aggregation="section", code="A")
        db.session.commit()

        for p in [p1, p2, p3]:
            response = self.client.get(url_for("metadata.products",
                                               aggregation=p.aggregation))
            self.assert_200(response)

            response_json = response.json["data"]
            self.assertEquals(len(response_json), 1)
            self.assertEquals(response_json[0]["code"], p.code)

        # TODO: do parent mapping properly
        response = self.client.get(url_for("metadata.products"))
        for item in response_json:
            # Find product object with given id
            obj = [x for x in [p1, p2, p3] if item["id"] == x.id][0]
            self.assertEquals(item["section_code"], obj.section_code)
            self.assertEquals(item["section_name"], obj.section_name)

        response = self.client.get(url_for("metadata.products"))
        self.assert_200(response)
        self.assertEquals(len(response.json["data"]), 3)

    def test_get_department(self):

        dept = factories.Department()
        db.session.commit()

        response = self.client.get(url_for("metadata.department",
                                           department_id=dept.id))
        self.assert_200(response)
        response_json = response.json["data"]
        self.assertEquals(response_json["code"], dept.code)
        self.assertEquals(response_json["name"], dept.name)
        self.assertEquals(response_json["population"], dept.population)
        self.assertEquals(response_json["gdp"], dept.gdp)

    def test_get_departments(self):

        factories.Department(code="22")
        factories.Department(code="24")
        factories.Department(code="26")
        db.session.commit()

        response = self.client.get(url_for("metadata.departments"))
        self.assert_200(response)

        response_json = response.json["data"]
        self.assertEquals(len(response_json), 3)
        self.assertEquals(set(x["code"] for x in response_json),
                          set(["22", "24", "26"]))

from flask import url_for

from colombia import factories
from colombia.core import db

from . import BaseTestCase


class TestMetadataAPIs(BaseTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def assert_metadata_api(self, api_url):

        response = self.client.get(api_url)
        self.assert_200(response)
        response_json = response.json["data"]

        for field in ["id", "code", "level", "parent_id"]:
            assert field in response_json

        return response_json

    def assert_json_matches_object(self, json, obj, fields_to_check):

        for field_name in fields_to_check:

            assert field_name in json
            assert hasattr(obj, field_name)

            field_value = getattr(obj, field_name)
            assert field_value == json[field_name]

    def test_get_hsproduct(self):
        product = factories.HSProduct(
            id=132,
            code="0302",
            level="4digit",
            parent_id=None,
            name_en="Fish, fresh or chilled, excluding fish fillets and other fish meat of heading 0304",
            name_short_en="Fish, excluding fillets",
            description_en="This is a description of fish.",
            name_es="El pescado, excepto los filetes"
        )
        db.session.commit()

        api_url = url_for("metadata.product",
                          product_id=product.id)

        response_json = self.assert_metadata_api(api_url)
        self.assert_json_matches_object(response_json, product,
                                        ["id", "code", "level", "parent_id",
                                         "name_en", "name_short_en",
                                         "description_en"])

    def test_get_hsproducts(self):
        p1 = factories.HSProduct(id=1, level="section", code="A",
                                 parent_id=None)
        p2 = factories.HSProduct(id=2, level="2digit", code="11", parent_id=1)
        p3 = factories.HSProduct(id=3, level="4digit", code="1108",
                                 parent_id=2)
        products = {1: p1, 2: p2, 3: p3}
        db.session.commit()

        response = self.client.get(url_for("metadata.products"))
        self.assert_200(response)

        response_json = response.json["data"]
        assert len(response_json) == 3

        for product_json in response_json:
            p = products[product_json["id"]]
            self.assert_json_matches_object(product_json, p,
                                            ["id", "code", "level",
                                             "parent_id", "name_en",
                                             "name_short_en",
                                             "description_en"])

    def test_get_hsproducts_levels(self):
        """Test that filtering by classification levels works."""

        p1 = factories.HSProduct(id=1, level="section", code="A", parent_id=None)
        p2 = factories.HSProduct(id=2, level="2digit", code="11", parent_id=1)
        p3 = factories.HSProduct(id=3, level="4digit", code="1108", parent_id=2)
        db.session.commit()

        for p in [p1, p2, p3]:
            response = self.client.get(url_for("metadata.products",
                                               level=p.level))
            self.assert_200(response)

            response_json = response.json["data"]
            self.assertEquals(len(response_json), 1)
            self.assert_json_matches_object(response_json[0], p,
                                            ["id", "code", "level",
                                             "parent_id", "name_en",
                                             "name_short_en",
                                             "description_en"])

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

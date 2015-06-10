from flask import Flask, url_for, request, Response
from unittest.mock import Mock
import pytest

from colombia import factories
from colombia.core import db
from colombia.data import routing

from . import BaseTestCase


class TestDataAPIs(BaseTestCase):

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


class TestDataRouting(BaseTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def test_extract_route_params(self):

        f = Flask("test_flask")

        with f.test_request_context("/product"):
            parameters = routing.extract_route_params(request)
            assert parameters == {}

        with f.test_request_context("/product?location=2&year=2007"):
            parameters = routing.extract_route_params(request)
            assert parameters == {"location": 2, "year": 2007}

        with f.test_request_context("/product/44?location=7&year=1963"):
            parameters = routing.extract_route_params(request)
            assert parameters == {"location": 7, "year": 1963}

        with f.test_request_context("/data/products/1234?from_location=37&to_location=44&year=235"):
            parameters = routing.extract_route_params(request)
            assert parameters == {"location": {"to": 44, "from": 37}, "year": 235}

        with f.test_request_context("/data/products/1234?to_location=44&year=235"):
            with pytest.raises(ValueError):
                parameters = routing.extract_route_params(request)

        with f.test_request_context("/data/industries/1234?from_location=44&year=235"):
            with pytest.raises(ValueError):
                parameters = routing.extract_route_params(request)

        with f.test_request_context("/data/industries/1234?derp=44&year=235"):
            with pytest.raises(ValueError):
                parameters = routing.extract_route_params(request)

    def test_lookup_classification_level(self):
        p1 = factories.HSProduct(level="2digit")
        p2 = factories.HSProduct(level="section")
        db.session.commit()

        p1_level = routing.lookup_classification_level("product", p1.id)
        p2_level = routing.lookup_classification_level("product", p2.id)

        assert p1_level == "2digit"
        assert p2_level == "section"

    def test_data_route_add(self):
        b = Flask("test_flask")
        routing.add_routes(b, {})

        assert "entity_handler_individual" in b.view_functions
        assert "entity_handler_many" in b.view_functions

    def test_data_route_match(self):
        endpoint = Mock(return_value="cats")

        f = self.app
        f.debug = True

        route = {
            "product": {
                (("location", "department"), ("year", None)): {
                    "name": "department_product_year",
                    "action": endpoint
                },
            },
            "year": {
            }
        }

        routing.add_routes(f, route)

        factories.Location(level="department", id=2)
        db.session.commit()

        with f.test_client() as c:
            response = c.get("/product?location=2&year=2007")
            assert response.status_code == 200
            assert response.data == b"cats"
            endpoint.assert_called_with(location=2, year=2007)

from flask import Flask, url_for, request

from colombia import factories
from colombia.core import db
from colombia.data import routing

from . import BaseTestCase

import pytest


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


    def test_data_routing(self):

        from unittest.mock import Mock
        endpoint = Mock(return_value="cats")
        endpoint.methods = ["GET"]
        endpoint.required_methods = ["GET"]

        from flask import Flask

        b = Flask("test_flask")

        route = {
            "product": {
                (("location", "department"), ("year", None)): {
                    "name": "department_product_year",
                    "action": endpoint
                },
            }
        }

        def add_routes(app, route):

            for entity_name, subroutes in route.items():
                def entity_endpoint(*args, **kwargs):

                    route["product"][(("location", "department"),("year", None))]["action"](location=2, department=7)
                    return "Blah"
                app.add_url_rule("/" + entity_name, entity_name,
                                 entity_endpoint, methods=["GET"])
                for route_keys, route_options in subroutes:
                    pass

        add_routes(b, route)

        assert "product" in b.view_functions

        response = b.test_client().get("/product?location=2&department=7")
        assert response.status_code == 200
        assert response.data == b"Blah"
        endpoint.assert_called_with(location=2, department=7)


from flask.ext.testing import TestCase

from colombia import create_app
from colombia import ext
from colombia.models import Municipality, HSProduct
from colombia.views import HSProductAPI, HSProductListAPI
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


class TestMetadataAPIs(ChassisTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def test_get_hsproduct(self):

        product = factories.HSProduct()
        db.session.commit()

        response = self.client.get(api.url_for(HSProductAPI,
                                               code=product.code))
        self.assert_200(response)

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
            self.assertEquals(response.json[0]["code"], p.code)

        response = self.client.get(api.url_for(HSProductListAPI))
        self.assert_200(response)
        self.assertEquals(len(response.json), 3)

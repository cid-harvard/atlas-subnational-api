from flask.ext.testing import TestCase

from colombia import create_app
from colombia import ext
from colombia.models import Municipality, HSProduct
from colombia.views import HSProductAPI
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

    def test_get_hsproducts(self):
        """Test to see if you can get a message by ID."""

        product = factories.HSProduct()
        db.session.commit()

        response = self.client.get(api.url_for(HSProductAPI,
                                               code=product.code))
        self.assert_200(response)

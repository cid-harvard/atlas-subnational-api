from flask.ext.testing import TestCase

from colombia import create_app
from colombia.models import db, Municipality, HSProduct
import factories


class ChassisTestCase(TestCase):
    """Base TestCase to add in convenience functions, defaults and custom
    asserts."""

    def create_app(self):
        return create_app({"SQLALCHEMY_DATABASE_URI":
                           self.SQLALCHEMY_DATABASE_URI})

    def setUp(self):
        db.create_all()

    def tearDown(self):
        db.session.remove()
        db.drop_all()


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


class TestCat(ChassisTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def test_get_cat(self):
        """Test to see if you can get a message by ID."""

        cat = factories.Cat()
        db.session.commit()

        response = self.client.get("/cats/" + str(cat.id))
        self.assert_200(response)
        resp_json = response.json
        self.assertEquals(resp_json["id"], str(cat.id))
        self.assertEquals(resp_json["born_at"], cat.born_at)
        self.assertEquals(resp_json["name"], cat.name)

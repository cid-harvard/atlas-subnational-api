from colombia import factories
from colombia.core import db
from colombia.models import Municipality, HSProduct, DepartmentProductYear

from . import BaseTestCase


class TestModels(BaseTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def try_model(self, factory, model):
        self.assertEquals(db.session.query(model).count(), 0)

        factory()
        db.session.commit()

        self.assertEquals(db.session.query(model).count(), 1)

    def test_locations(self):
        self.try_model(factories.Municipality, Municipality)

    def test_products(self):
        self.try_model(factories.HSProduct, HSProduct)

    def test_DepartmentProductYear(self):
        self.try_model(factories.DepartmentProductYear, DepartmentProductYear)




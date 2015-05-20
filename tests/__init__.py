from colombia import create_app

from atlas_core.testing import BaseTestCase as CoreBaseTestCase


class BaseTestCase(CoreBaseTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://"

    def create_app(self):
        return create_app({"SQLALCHEMY_DATABASE_URI":
                           self.SQLALCHEMY_DATABASE_URI,
                           "TESTING": True})

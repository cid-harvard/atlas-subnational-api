from ..core import db
from . import models
from ..metadata.factories import Location, HSProduct


from faker import Factory as Fake
from factory.alchemy import SQLAlchemyModelFactory
import factory
from factory import fuzzy

import logging

# Suppress factory-boy debug data
factory_log = logging.getLogger("factory")
factory_log.setLevel(logging.WARNING)

faker = Fake.create()


class DepartmentProductYear(SQLAlchemyModelFactory):
    class Meta:
        model = models.DepartmentProductYear
        sqlalchemy_session = db.session

    id = factory.Sequence(lambda n: n)

    department = factory.SubFactory(Location)
    product = factory.SubFactory(HSProduct)
    year = fuzzy.FuzzyInteger(1999, 2013)

    import_value = fuzzy.FuzzyInteger(10**5, 10**11)
    export_value = fuzzy.FuzzyInteger(10**5, 10**11)
    export_rca = fuzzy.FuzzyFloat(0, 1)
    density = fuzzy.FuzzyFloat(0, 1)
    cog = fuzzy.FuzzyFloat(-1, 1)
    coi = fuzzy.FuzzyFloat(-1, 1)

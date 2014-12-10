from colombia.models import db
from colombia import models

from faker import Factory as Fake
from factory.alchemy import SQLAlchemyModelFactory
import factory
from factory import fuzzy

import logging

# Suppress factory-boy debug data
factory_log = logging.getLogger("factory")
factory_log.setLevel(logging.WARNING)

faker = Fake.create()


class HSProduct(SQLAlchemyModelFactory):
    class Meta:
        model = models.HSProduct
        sqlalchemy_session = db.session

    id = factory.Sequence(lambda n: n)
    aggregation = fuzzy.FuzzyChoice(models.HSProduct.AGGREGATIONS)
    name = fuzzy.FuzzyChoice(["Petroleum", "Horses", "Cut flowers", "Gold",
                              "Cars", "Packaged medicaments", "Soya beans",
                              "Coal", "T-shirts", "Refrigerators", "Nuts"])
    code = fuzzy.FuzzyInteger(0, 9999)


class Municipality(SQLAlchemyModelFactory):
    class Meta:
        model = models.Municipality
        sqlalchemy_session = db.session

    id = factory.Sequence(lambda n: n)
    name = factory.LazyAttribute(lambda x: faker.city())
    code = fuzzy.FuzzyInteger(10000, 99999)
    aggregation = "municipality"

    size = fuzzy.FuzzyChoice(models.Municipality.SIZE)
    population = fuzzy.FuzzyInteger(1000, 12000000)
    nbi = fuzzy.FuzzyDecimal(0, 1)


class Department(SQLAlchemyModelFactory):
    class Meta:
        model = models.Department
        sqlalchemy_session = db.session

    id = factory.Sequence(lambda n: n)
    name = factory.LazyAttribute(lambda x: faker.state())
    code = fuzzy.FuzzyInteger(10, 99)
    aggregation = "department"

    population = fuzzy.FuzzyInteger(1000, 12000000)
    gdp = fuzzy.FuzzyInteger(1000000, 12000000000)


class DepartmentProductYear(SQLAlchemyModelFactory):
    class Meta:
        model = models.DepartmentProductYear
        sqlalchemy_session = db.session

    id = factory.Sequence(lambda n: n)

    department = factory.SubFactory(Department)
    product = factory.SubFactory(HSProduct)
    year = fuzzy.FuzzyInteger(1999, 2013)

    import_value = fuzzy.FuzzyInteger(10**5, 10**11)
    export_value = fuzzy.FuzzyInteger(10**5, 10**11)
    export_rca = fuzzy.FuzzyFloat(0, 1)
    distance = fuzzy.FuzzyFloat(0, 8)
    opp_gain = fuzzy.FuzzyFloat(-1, 1)

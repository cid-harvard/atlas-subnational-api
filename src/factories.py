from colombia.models import db
from colombia import models

from faker import Factory as Fake
from factory.alchemy import SQLAlchemyModelFactory
import factory
from factory import fuzzy

import logging

#Suppress factory-boy debug data
factory_log = logging.getLogger("factory")
factory_log.setLevel(logging.WARNING)

faker = Fake.create()


class Cat(SQLAlchemyModelFactory):
    FACTORY_FOR = models.Cat
    FACTORY_SESSION = db.session

    id = factory.LazyAttribute(lambda x: faker.unix_time())
    born_at = factory.LazyAttribute(lambda x: faker.unix_time())

    name = factory.LazyAttribute(lambda x: faker.first_name())


class HSProduct(SQLAlchemyModelFactory):
    class Meta:
        model = models.HSProduct
        sqlalchemy_session = db.session

    id = factory.Sequence(lambda n: n)
    aggregation = fuzzy.FuzzyChoice(models.HSProduct.AGGREGATIONS)
    name = fuzzy.FuzzyChoice(["Petroleum", "Horses", "Cut flowers", "Gold",
                              "Cars", "Packaged medicaments", "Soya beans",
                              "Coal", "T-shirts", "Refrigerators", "Nuts"])


class Municipality(SQLAlchemyModelFactory):
    class Meta:
        model = models.Municipality
        sqlalchemy_session = db.session

    id = factory.Sequence(lambda n: n)
    name = factory.LazyAttribute(lambda x: faker.city())
    code = factory.Sequence(lambda n: n)
    aggregation = fuzzy.FuzzyChoice(models.Municipality.AGGREGATIONS)

    size = fuzzy.FuzzyChoice(models.Municipality.SIZE)
    population = fuzzy.FuzzyInteger(1000, 12000000)
    nbi = fuzzy.FuzzyDecimal(0, 1)


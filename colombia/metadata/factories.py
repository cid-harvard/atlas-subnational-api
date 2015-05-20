from ..core import db
from . import models

from faker import Factory as Fake
from factory.alchemy import SQLAlchemyModelFactory
import factory
from factory import fuzzy

import logging
import random

# Suppress factory-boy debug data
factory_log = logging.getLogger("factory")
factory_log.setLevel(logging.WARNING)

faker = Fake.create()


PRODUCT_NAMES = ["Petroleum", "Horses", "Cut flowers", "Gold", "Cars",
                 "Packaged medicaments", "Soya beans", "Coal", "T-shirts",
                 "Refrigerators", "Nuts", "X-Ray Machines", "Aircraft",
                 "Potatoes", "Coffee", "Chairs", "Springs", "Alcohol", "Shoes",
                 "Shirts", "Paper"]
random.shuffle(PRODUCT_NAMES)


class HSProduct(SQLAlchemyModelFactory):
    class Meta:
        model = models.HSProduct
        sqlalchemy_session = db.session

    id = factory.Sequence(lambda n: n)
    code = factory.Sequence(lambda n: str(1000+n))
    level = fuzzy.FuzzyChoice(models.HSProduct.LEVELS)
    parent_id = None

    name_en = "A very very very very very long english name"
    name_short_en = factory.Sequence(lambda n: PRODUCT_NAMES[n % len(PRODUCT_NAMES)])
    description_en = "A very very very long description in english."


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


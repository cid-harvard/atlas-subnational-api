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


class Metadata(SQLAlchemyModelFactory):
    class Meta:
        sqlalchemy_session = db.session

    id = factory.Sequence(lambda n: n)
    code = factory.Sequence(lambda n: str(1000+n))
    level = "a_classification_level"
    parent_id = None

    name_en = "A very very very very very long english name"
    name_short_en = "A short name"
    description_en = "A very very very long description in english."


class HSProduct(Metadata):
    class Meta:
        model = models.HSProduct

    level = fuzzy.FuzzyChoice(models.HSProduct.LEVELS)
    name_short_en = factory.Sequence(lambda n: PRODUCT_NAMES[n % len(PRODUCT_NAMES)])


class Location(Metadata):
    class Meta:
        model = models.Location

    level = fuzzy.FuzzyChoice(models.Location.LEVELS)
    name_short_en = factory.LazyAttribute(lambda x: faker.city())

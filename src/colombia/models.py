from flask.ext.sqlalchemy import SQLAlchemy
from sqlalchemy.ext.hybrid import hybrid_method

import time
import random

db = SQLAlchemy()


def new_cat_name(prefix="mittens"):
    """Returns a random cat name."""
    return "%s%d" % (prefix, random.randint(0, 9999))


class BaseModel(db.Model):
    __abstract__ = True
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }


class IDMixin:
    """Adds in an autoincremented integer ID."""
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)


class LanguagesMixin:
    """"
    Mixin to include language support in a database object, plus convenience
    functions.

    - TODO: Write a make_languages(lang_list, string_length) to have this not
    be hardcoded values.
    """
    en = db.Column(db.String(50))
    es = db.Column(db.String(50))

    @hybrid_method
    def localized_name(self, lang):
        return getattr(self, lang)


class Cat(BaseModel):
    """
    A cat is a silly animal that belongs on the internet.

    Read more about :ref:`cat_architecture` before you
    make any large changes. Do not try to shoehorn in tangential features,
    create a new model instead!
    """
    __tablename__ = "cat"

    #: Unique cat ID
    id = db.Column(db.Integer, primary_key=True)

    #: UTC creation stamp
    born_at = db.Column(db.Integer, nullable=False, default=time.time)

    #: Name of the cat
    name = db.Column(db.String,
                     nullable=False,
                     default=new_cat_name,
                     unique=True)



class HSProduct(BaseModel, IDMixin, LanguagesMixin):
    """A product according to the HS4 (Harmonized System) classification.
    Details can be found here: http://www.wcoomd.org/en/topics/nomenclature/instrument-and-tools/hs_nomenclature_2012/hs_nomenclature_table_2012.aspx
    """
    __tablename__ = "product"

    AGGREGATIONS = [
        "section",
        "2digit",
        "4digit"
    ]
    aggregation = db.Column(db.Enum(*AGGREGATIONS))

    name = db.Column(db.String(50))


class Location(BaseModel, IDMixin):
    __tablename__ = "location"

    AGGREGATIONS = [
        "municipality",
        "department",
    ]
    aggregation = db.Column(db.Enum(*AGGREGATIONS))

    name = db.Column(db.String(50))
    code = db.Column(db.String(5))

    SIZE = [
        "city",
        "midsize",
        "rural"
    ]
    aggregation = db.Column(db.Enum(*SIZE))

    pop_2012 = db.Column(db.Integer)
    nbi = db.Column(db.Numeric)

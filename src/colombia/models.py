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

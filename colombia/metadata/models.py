from atlas_core.sqlalchemy import BaseModel
from atlas_core.model_mixins import IDMixin, LanguageMixin

from ..core import db


# On hierarchy trees: there are two types. One is where each element is the
# same regardless of position in the hierarchy, the other has different
# elements for each level of hierarchy.



class I18nMixinBase(object):


    #name_en = db.Column(db.UnicodeText)
    #name_short_en = db.Column(db.Unicode(50))
    #description_en = db.Column(db.UnicodeText)

    #@hybrid_method
    def get_localized(self, field, lang):
        """Look up the language localized version of a field by looking up
        field_lang."""
        return getattr(self, field + "_" + lang)

    @staticmethod
    def create(fields, languages=["en"], class_name="I18nMixin"):
        localized_fields = {}
        for name, value in fields.items():
            for language in languages:
                field_name = name + "_" + language
                localized_fields[field_name] = db.Column(value)
        return type(class_name, (object,), localized_fields)


I18nMixin = I18nMixinBase.create(
    languages=["en", "es", "de"],
    fields={
        "name": db.UnicodeText,
        "name_short": db.Unicode(50),
        "description": db.UnicodeText
    })


class Metadata(BaseModel, IDMixin, I18nMixin):
    """Baseclass for all entity metadata models. Any subclass of this class
    must have two fields:
        - a LEVELS = [] list that contains all the classification levels as
        strings
        - a db.Column(db.Enum(*LEVELS)) enum field
    """

    __abstract__ = True

    code = db.Column(db.Unicode(25))
    parent_id = db.Column(db.Integer)


class HSProduct(Metadata):
    """A product according to the HS4 (Harmonized System) classification.
    Details can be found here: http://www.wcoomd.org/en/topics/nomenclature/instrument-and-tools/hs_nomenclature_2012/hs_nomenclature_table_2012.aspx
    """
    __tablename__ = "product"

    #: Possible aggregation levels
    LEVELS = [
        "section",
        "2digit",
        "4digit"
    ]
    level = db.Column(db.Enum(*LEVELS))


class Location(BaseModel, IDMixin):
    """A geographical location."""
    __tablename__ = "location"
    type = db.Column(db.String(10))
    __mapper_args__ = {
        'polymorphic_identity': 'location',
        'polymorphic_on': type
    }

    #: Possible aggregation levels
    AGGREGATIONS = [
        "municipality",
        "department",
    ]
    #: Enum that contains level of aggregation - municipalities, cities,
    #: regions, departments
    aggregation = db.Column(db.Enum(*AGGREGATIONS))

    #: Name of the location in the most common language
    name = db.Column(db.String(50))

    #: Location code - zip code or DANE code, etc
    code = db.Column(db.String(5))


class Municipality(Location):
    """A municipality that has a 5-digit code. Cities often contain multiple
    municipalities, but there are also standalone municipalities that are not
    part of any city."""

    __tablename__ = "municipality"
    __mapper_args__ = {
        'polymorphic_identity': 'municipality',
    }

    id = db.Column(db.Integer,
                   db.ForeignKey('location.id'), primary_key=True)

    #: Possible sizes of a municipality
    SIZE = [
        "city",
        "midsize",
        "rural"
    ]
    #: Size of the municipality
    size = db.Column(db.Enum(*SIZE))

    population = db.Column(db.Integer)
    nbi = db.Column(db.Numeric)


class Department(Location):
    """A grouping of municipalities to create 32ish areas of the country.
    Departments in Colombia have 2 digit codes, which are the first 2 digits of
    the 5-digit codes of the constituent municipalities."""

    __tablename__ = "department"
    __mapper_args__ = {
        'polymorphic_identity': 'department',
    }

    id = db.Column(db.Integer,
                   db.ForeignKey('location.id'), primary_key=True)

    population = db.Column(db.Integer)
    gdp = db.Column(db.Integer)



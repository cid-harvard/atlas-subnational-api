from flask import abort
from sqlalchemy.ext.hybrid import hybrid_method
from flask.ext import restful

from colombia import ext

db = ext.db


class BaseQuery(db.Query):

    def get_or_abort(self, obj_id, http_code=404):
        """Get an object or return an error code."""
        result = self.get(obj_id)
        return result or abort(http_code)

    def first_or_abort(self, obj_id, http_code=404):
        """Get first result or return an error code."""
        result = self.first()
        return result or abort(http_code)

    def filter_by_enum(self, enum, value, possible_values=None, http_code=400):
        """
        Filters a query object by an enum, testing that it got a valid value.

        :param enum: Enum column from model, e.g. Vehicle.type
        :param value: Value to filter by
        :param possible_values: None or list of acceptable values for `value`
        """
        if value is None:
            return self

        if possible_values is None:
            possible_values = enum.property.columns[0].type.enums

        if value not in possible_values:
            msg = "Expected one of: {0}, got {1}"\
                .format(possible_values, value)
            restful.abort(http_code, message=msg)

        return self.filter(enum == value)


class BaseModel(db.Model):
    __abstract__ = True
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }
    query_class = BaseQuery


class IDMixin:
    """Adds in an autoincremented integer ID."""
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    def __repr__(self):
        return "<{0}: {1}>".format(self.__class__.__name__, self.id)


class LanguageMixin:
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


class HSProduct(BaseModel, IDMixin, LanguageMixin):
    """A product according to the HS4 (Harmonized System) classification.
    Details can be found here: http://www.wcoomd.org/en/topics/nomenclature/instrument-and-tools/hs_nomenclature_2012/hs_nomenclature_table_2012.aspx
    """
    __tablename__ = "product"

    #: Possible aggregation levels
    AGGREGATIONS = [
        "section",
        "2digit",
        "4digit"
    ]
    #: Enum that contains level of aggregation - how many "digits" of detail
    aggregation = db.Column(db.Enum(*AGGREGATIONS))

    #: Canonical name of the product - in non_colloquial english (i.e. name vs
    #: name_en)
    name = db.Column(db.String(50))

    #: HS4 code of the product, in the level of aggregation described in
    #: :py:class:`.aggregation`.
    code = db.Column(db.String(6))

    def __repr__(self):
        return "<HSProduct: %d, %s>" % (self.id, self.name)


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
    """A municipality that has a 5-digit code."""

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

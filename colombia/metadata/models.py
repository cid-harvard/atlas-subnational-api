from atlas_core.sqlalchemy import BaseModel
from atlas_core.model_mixins import IDMixin

from ..core import db

from sqlalchemy.ext.hybrid import hybrid_method

# On hierarchy trees: there are two types. One is where each element is the
# same regardless of position in the hierarchy, the other has different
# elements for each level of hierarchy.



class I18nMixinBase(object):


    #name_en = db.Column(db.UnicodeText)
    #name_short_en = db.Column(db.Unicode(50))
    #description_en = db.Column(db.UnicodeText)

    @hybrid_method
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
        return type(class_name, (I18nMixinBase,), localized_fields)


I18nMixin = I18nMixinBase.create(
    languages=["en", "es", "de"],
    fields={
        "name": db.UnicodeText,
        "name_short": db.Unicode(75),
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


product_levels = [
    "section",
    "2digit",
    "4digit"
]
product_enum = db.Enum(*product_levels, name="product_level")

location_levels = [
    "country",
    "department",
    "msa",
    "municipality",
]
location_enum = db.Enum(*location_levels, name="location_level")

industry_levels = [
    "section",
    "division",
    "group",
    "class"
]
industry_enum = db.Enum(*industry_levels, name="industry_level")

country_levels = [
    "region",
    "country"
]
country_enum = db.Enum(*country_levels, name="country_level")

occupation_levels = [
    "major_group",
    "minor_group",
    "broad_occupation",
    "detailed_occupation",
]
occupation_enum = db.Enum(*occupation_levels, name="occupation_level")


livestock_levels = [
    "level0",
    "level1",
]
livestock_enum = db.Enum(*livestock_levels, name="livestock_level")


agproduct_levels = [
    "level0",
    "level1",
    "level2",
    "level3",
]
agproduct_enum = db.Enum(*agproduct_levels, name="agproduct_level")


class HSProduct(Metadata):
    """A product according to the HS4 (Harmonized System) classification.
    Details can be found here: http://www.wcoomd.org/en/topics/nomenclature/instrument-and-tools/hs_nomenclature_2012/hs_nomenclature_table_2012.aspx
    """
    __tablename__ = "product"

    #: Possible aggregation levels
    LEVELS = product_levels
    level = db.Column(product_enum)


class Location(Metadata):
    """A geographical location. Locations have multiple levels:

    A municipality is the smallest unit of location we have and has a 5-digit
    code. Cities often contain multiple municipalities, but there are also
    standalone municipalities that are not part of any city.

    A department is a grouping of municipalities to create 32ish areas of the
    country. Departments in Colombia have 2 digit codes, which are the first 2
    digits of the 5-digit codes of the constituent municipalities."""
    __tablename__ = "location"

    #: Possible aggregation levels
    LEVELS = location_levels
    level = db.Column(location_enum)


class Industry(Metadata):
    """An ISIC 4 industry."""
    __tablename__ = "industry"

    #: Possible aggregation levels
    LEVELS = industry_levels
    level = db.Column(industry_enum)


class Occupation(Metadata):
    """An occupation."""
    __tablename__ = "occupation"

    #: Possible aggregation levels
    LEVELS = occupation_levels
    level = db.Column(occupation_enum)


class Country(Metadata):
    """A country."""
    __tablename__ = "country"

    #: Possible aggregation levels
    LEVELS = country_levels
    level = db.Column(country_enum)


class Livestock(Metadata):
    __tablename__ = "livestock"

    #: Possible aggregation levels
    LEVELS = livestock_levels
    level = db.Column(livestock_enum)


class AgriculturalProduct(Metadata):
    __tablename__ = "agproduct"

    #: Possible aggregation levels
    LEVELS = agproduct_levels
    level = db.Column(agproduct_enum)

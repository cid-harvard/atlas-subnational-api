from sqlalchemy.ext.hybrid import hybrid_property

from atlas_core import db
from atlas_core.sqlalchemy import BaseModel
from atlas_core.model_mixins import IDMixin, LanguageMixin


# On hierarchy trees: there are two types. One is where each element is the
# same regardless of position in the hierarchy, the other has different
# elements for each level of hierarchy.


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

    #: HS4 section of the product, this is deprecated and shouldn't be used.
    #: EVER. I had to stick this in for the alpha demo.
    section_code = db.Column(db.String(6))
    #: HS4 section name of the product, this is deprecated and shouldn't be used.
    #: EVER. I had to stick this in for the alpha demo.
    section_name = db.Column(db.String(6))
    section_name_es = db.Column(db.String(6))

    def __repr__(self):
        return "<HSProduct: %d, %s>" % (self.id or -1, self.name or None)


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


class DepartmentProductYear(BaseModel, IDMixin):

    __tablename__ = "department_product_year"

    department_id = db.Column(db.Integer, db.ForeignKey(Department.id))
    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)

    department = db.relationship(Department)
    product = db.relationship(HSProduct)

    import_value = db.Column(db.Integer)
    export_value = db.Column(db.Integer)
    export_rca = db.Column(db.Integer)
    density = db.Column(db.Float)
    cog = db.Column(db.Float)
    coi = db.Column(db.Float)

    @hybrid_property
    def distance(self):
        return (1.0 - self.density).label("distance")


class DepartmentYear(BaseModel, IDMixin):

    __tablename__ = "department_year"

    department_id = db.Column(db.Integer, db.ForeignKey(Department.id))
    year = db.Column(db.Integer)

    department = db.relationship(Department)

    eci = db.Column(db.Float)
    eci_rank = db.Column(db.Integer)
    diversity = db.Column(db.Float)


class ProductYear(BaseModel, IDMixin):

    __tablename__ = "product_year"

    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)

    product = db.relationship(HSProduct)

    pci = db.Column(db.Float)
    pci_rank = db.Column(db.Integer)

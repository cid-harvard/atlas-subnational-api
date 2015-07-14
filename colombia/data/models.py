from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import func

from atlas_core.sqlalchemy import BaseModel
from atlas_core.model_mixins import IDMixin

from ..core import db

from ..metadata.models import (Location, HSProduct, Industry)


class DepartmentProductYear(BaseModel, IDMixin):

    __tablename__ = "department_product_year"

    department_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)

    department = db.relationship(Location)
    product = db.relationship(HSProduct)

    import_value = db.Column(db.Integer)
    export_value = db.Column(db.Integer)

    export_rca = db.Column(db.Integer)
    density = db.Column(db.Float)
    cog = db.Column(db.Float)
    coi = db.Column(db.Float)

    @hybrid_property
    def distance(self):
        if self.density is None:
            return None
        return 1.0 - self.density

    @distance.expression
    def distance(cls):
        return (1.0 - cls.density).label("distance")


class DepartmentYear(BaseModel, IDMixin):

    __tablename__ = "department_year"

    department_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    year = db.Column(db.Integer)

    department = db.relationship(Location)

    eci = db.Column(db.Float)
    eci_rank = db.Column(db.Integer)
    diversity = db.Column(db.Float)

    gdp_nominal = db.Column(db.Integer)
    gdp_real = db.Column(db.Integer)
    gdp_pc_nominal = db.Column(db.Integer)
    gdp_pc_real = db.Column(db.Integer)

    population = db.Column(db.Integer)

class ProductYear(BaseModel, IDMixin):

    __tablename__ = "product_year"

    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)

    product = db.relationship(HSProduct)

    pci = db.Column(db.Float)
    pci_rank = db.Column(db.Integer)


class IndustryYear(BaseModel, IDMixin):

    __tablename__ = "industry_year"

    industry_id = db.Column(db.Integer, db.ForeignKey(Industry.id))
    year = db.Column(db.Integer)

    industry = db.relationship(Industry)

    complexity = db.Column(db.Float)


class DepartmentIndustryYear(BaseModel, IDMixin):

    __tablename__ = "department_industry_year"

    department_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    industry_id = db.Column(db.Integer, db.ForeignKey(Industry.id))
    year = db.Column(db.Integer)

    department = db.relationship(Location)
    industry = db.relationship(Industry)

    employment = db.Column(db.Integer)
    wages = db.Column(db.Integer)

    rca = db.Column(db.Integer)
    density = db.Column(db.Float)
    cog = db.Column(db.Float)
    coi = db.Column(db.Float)

    @hybrid_property
    def distance(self):
        if self.density is None:
            return None
        return 1.0 - self.density

    @distance.expression
    def distance(cls):
        return (1.0 - cls.density).label("distance")


class MunicipalityIndustryYear(BaseModel, IDMixin):

    __tablename__ = "municipality_industry_year"

    municipality_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    industry_id = db.Column(db.Integer, db.ForeignKey(Industry.id))
    year = db.Column(db.Integer)

    municipality = db.relationship(Location)
    industry = db.relationship(Industry)

    employment = db.Column(db.Integer)
    wages = db.Column(db.Integer)

    rca = db.Column(db.Integer)
    density = db.Column(db.Float)
    cog = db.Column(db.Float)
    coi = db.Column(db.Float)

    @hybrid_property
    def distance(self):
        if self.density is None:
            return None
        return 1.0 - self.density

    @distance.expression
    def distance(cls):
        return (1.0 - cls.density).label("distance")

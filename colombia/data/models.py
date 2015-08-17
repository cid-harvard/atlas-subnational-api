from sqlalchemy.ext.hybrid import hybrid_property

from atlas_core.sqlalchemy import BaseModel
from atlas_core.model_mixins import IDMixin

from ..core import db

from ..metadata.models import (Location, HSProduct, Industry, product_enum,
                               industry_enum)


class DepartmentProductYear(BaseModel, IDMixin):

    __tablename__ = "department_product_year"

    department_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)
    level = db.Column(product_enum)

    department = db.relationship(Location)
    product = db.relationship(HSProduct)

    export_value = db.Column(db.BIGINT)
    import_value = db.Column(db.BIGINT)
    export_num_plants = db.Column(db.Integer)
    import_num_plants = db.Column(db.Integer)

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


class MSAProductYear(BaseModel, IDMixin):

    __tablename__ = "msa_product_year"

    msa_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)
    level = db.Column(product_enum)

    msa = db.relationship(Location)
    product = db.relationship(HSProduct)

    export_value = db.Column(db.BIGINT)
    import_value = db.Column(db.BIGINT)
    export_num_plants = db.Column(db.Integer)
    import_num_plants = db.Column(db.Integer)

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


class MunicipalityProductYear(BaseModel, IDMixin):

    __tablename__ = "municipality_product_year"

    municipality_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)
    level = db.Column(product_enum)

    municipality = db.relationship(Location)
    product = db.relationship(HSProduct)

    export_value = db.Column(db.BIGINT)
    import_value = db.Column(db.BIGINT)
    export_num_plants = db.Column(db.Integer)
    import_num_plants = db.Column(db.Integer)

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


class CountryDepartmentProductYear(BaseModel, IDMixin):

    __tablename__ = "country_department_product_year"

    country_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    department_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)
    level = db.Column(product_enum)

    department = db.relationship(Location, foreign_keys=[department_id])
    product = db.relationship(HSProduct)

    export_value = db.Column(db.BIGINT)
    num_plants = db.Column(db.Integer)


class CountryMunicipalityProductYear(BaseModel, IDMixin):

    __tablename__ = "country_municipality_product_year"

    country_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    municipality_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)
    level = db.Column(product_enum)

    municipality = db.relationship(Location, foreign_keys=[municipality_id])
    product = db.relationship(HSProduct)

    export_value = db.Column(db.BIGINT)
    num_plants = db.Column(db.Integer)


class DepartmentYear(BaseModel, IDMixin):

    __tablename__ = "department_year"

    department_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    year = db.Column(db.Integer)

    department = db.relationship(Location)

    eci = db.Column(db.Float)
    eci_rank = db.Column(db.Integer)
    diversity = db.Column(db.Float)

    gdp_nominal = db.Column(db.BIGINT)
    gdp_real = db.Column(db.BIGINT)
    gdp_pc_nominal = db.Column(db.Integer)
    gdp_pc_real = db.Column(db.Integer)

    population = db.Column(db.Integer)

    employment = db.Column(db.Integer)
    wages = db.Column(db.BIGINT)
    monthly_wages = db.Column(db.Integer)
    num_establishments = db.Column(db.Integer)


class ProductYear(BaseModel, IDMixin):

    __tablename__ = "product_year"

    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    year = db.Column(db.Integer)
    level = db.Column(product_enum)

    product = db.relationship(HSProduct)

    pci = db.Column(db.Float)
    pci_rank = db.Column(db.Integer)

    export_value = db.Column(db.BIGINT)
    import_value = db.Column(db.BIGINT)
    export_num_plants = db.Column(db.Integer)
    import_num_plants = db.Column(db.Integer)


class IndustryYear(BaseModel, IDMixin):

    __tablename__ = "industry_year"

    industry_id = db.Column(db.Integer, db.ForeignKey(Industry.id))
    year = db.Column(db.Integer)
    level = db.Column(industry_enum)

    industry = db.relationship(Industry)

    employment = db.Column(db.Integer)
    wages = db.Column(db.BIGINT)
    monthly_wages = db.Column(db.Integer)
    num_establishments = db.Column(db.Integer)

    complexity = db.Column(db.Float)


class DepartmentIndustryYear(BaseModel, IDMixin):

    __tablename__ = "department_industry_year"

    department_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    industry_id = db.Column(db.Integer, db.ForeignKey(Industry.id))
    year = db.Column(db.Integer)
    level = db.Column(industry_enum)

    department = db.relationship(Location)
    industry = db.relationship(Industry)

    employment = db.Column(db.Integer)
    wages = db.Column(db.BIGINT)
    monthly_wages = db.Column(db.Integer)
    num_establishments = db.Column(db.Integer)

    rca = db.Column(db.Integer)
    distance = db.Column(db.Float)
    cog = db.Column(db.Float)
    coi = db.Column(db.Float)


class MSAIndustryYear(BaseModel, IDMixin):

    __tablename__ = "msa_industry_year"

    msa_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    industry_id = db.Column(db.Integer, db.ForeignKey(Industry.id))
    year = db.Column(db.Integer)
    level = db.Column(industry_enum)

    msa = db.relationship(Location)
    industry = db.relationship(Industry)

    employment = db.Column(db.Integer)
    wages = db.Column(db.BIGINT)
    monthly_wages = db.Column(db.Integer)
    num_establishments = db.Column(db.Integer)

    rca = db.Column(db.Integer)
    distance = db.Column(db.Float)
    cog = db.Column(db.Float)
    coi = db.Column(db.Float)


class MunicipalityIndustryYear(BaseModel, IDMixin):

    __tablename__ = "municipality_industry_year"

    municipality_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    industry_id = db.Column(db.Integer, db.ForeignKey(Industry.id))
    year = db.Column(db.Integer)
    level = db.Column(industry_enum)

    municipality = db.relationship(Location)
    industry = db.relationship(Industry)

    employment = db.Column(db.Integer)
    wages = db.Column(db.BIGINT)
    monthly_wages = db.Column(db.Integer)
    num_establishments = db.Column(db.Integer)

    rca = db.Column(db.Integer)
    distance = db.Column(db.Float)
    cog = db.Column(db.Float)
    coi = db.Column(db.Float)

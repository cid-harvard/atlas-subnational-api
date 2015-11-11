from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property

from atlas_core.sqlalchemy import BaseModel
from atlas_core.model_mixins import IDMixin

from ..core import db

from ..metadata.models import (Location, HSProduct, Industry, Occupation,
                               product_enum, industry_enum, occupation_enum)


class XProductYear(BaseModel, IDMixin):

    __abstract__ = True

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def product_id(cls):
        return db.Column(db.Integer, db.ForeignKey(HSProduct.id))

    year = db.Column(db.Integer)
    level = db.Column(product_enum)

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


class CountryProductYear(XProductYear):

    __tablename__ = "country_product_year"


class DepartmentProductYear(XProductYear):

    __tablename__ = "department_product_year"


class MSAProductYear(XProductYear):

    __tablename__ = "msa_product_year"


class MunicipalityProductYear(XProductYear):

    __tablename__ = "municipality_product_year"


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


class CountryXProductYear(BaseModel, IDMixin):

    __abstract__ = True

    @declared_attr
    def country_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def product_id(cls):
        return db.Column(db.Integer, db.ForeignKey(HSProduct.id))

    year = db.Column(db.Integer)
    level = db.Column(product_enum)

    export_value = db.Column(db.BIGINT)
    export_num_plants = db.Column(db.Integer)


class CountryDepartmentProductYear(CountryXProductYear):

    __tablename__ = "country_department_product_year"


class CountryMunicipalityProductYear(CountryXProductYear):

    __tablename__ = "country_municipality_product_year"


class CountryXYear(BaseModel, IDMixin):

    __abstract__ = True

    @declared_attr
    def country_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    year = db.Column(db.Integer)

    export_value = db.Column(db.BIGINT)
    export_num_plants = db.Column(db.Integer)


class CountryCountryYear(CountryXYear):

    __tablename__ = "country_country_year"


class CountryDepartmentYear(CountryXYear):

    __tablename__ = "country_department_year"


class CountryMSAYear(CountryXYear):

    __tablename__ = "country_msa_year"


class DepartmentYear(BaseModel, IDMixin):

    __tablename__ = "department_year"

    location_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    year = db.Column(db.Integer)

    location = db.relationship(Location)

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


class MSAYear(BaseModel, IDMixin):

    __tablename__ = "msa_year"

    location_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    year = db.Column(db.Integer)

    location = db.relationship(Location)

    eci = db.Column(db.Float)


class XIndustryYear(BaseModel, IDMixin):

    __abstract__ = True

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def industry_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Industry.id))

    year = db.Column(db.Integer)
    level = db.Column(industry_enum)

    employment = db.Column(db.Integer)
    wages = db.Column(db.BIGINT)
    monthly_wages = db.Column(db.Integer)
    num_establishments = db.Column(db.Integer)

    rca = db.Column(db.Integer)
    distance = db.Column(db.Float)
    cog = db.Column(db.Float)
    coi = db.Column(db.Float)


class CountryIndustryYear(XIndustryYear):

    __tablename__ = "country_industry_year"


class DepartmentIndustryYear(XIndustryYear):

    __tablename__ = "department_industry_year"


class MSAIndustryYear(XIndustryYear):

    __tablename__ = "msa_industry_year"


class MunicipalityIndustryYear(XIndustryYear):

    __tablename__ = "municipality_industry_year"


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


class OccupationYear(BaseModel, IDMixin):

    __tablename__ = "occupation_year"

    occupation_id = db.Column(db.Integer, db.ForeignKey(Occupation.id))
    level = db.Column(occupation_enum)

    occupation = db.relationship(Occupation)

    average_wages = db.Column(db.Integer)
    num_vacancies = db.Column(db.Integer)


class OccupationIndustryYear(BaseModel, IDMixin):

    __tablename__ = "occupation_industry_year"

    occupation_id = db.Column(db.Integer, db.ForeignKey(Occupation.id))
    industry_id = db.Column(db.Integer, db.ForeignKey(Industry.id))
    level = db.Column(occupation_enum)

    occupation = db.relationship(Occupation)
    industry = db.relationship(Industry)

    average_wages = db.Column(db.Integer)
    num_vacancies = db.Column(db.Integer)

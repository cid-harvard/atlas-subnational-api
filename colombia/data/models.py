from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property

from atlas_core.sqlalchemy import BaseModel
from atlas_core.model_mixins import IDMixin

from ..core import db

from ..metadata.models import (Location, HSProduct, Industry, Occupation,
                               Country, Livestock, AgriculturalProduct,
                               LandUse, FarmType, FarmSize,
                               NonagriculturalActivity)

from ..metadata.models import (product_enum, industry_enum, occupation_enum,
                               livestock_enum, agproduct_enum, land_use_enum,
                               farmtype_enum, farmsize_enum, nonag_enum)


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
    import_value = db.Column(db.BIGINT)
    import_num_plants = db.Column(db.Integer)


class CountryDepartmentProductYear(CountryXProductYear):

    __tablename__ = "country_department_product_year"


class CountryMSAProductYear(CountryXProductYear):

    __tablename__ = "country_msa_product_year"


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
    import_value = db.Column(db.BIGINT)
    import_num_plants = db.Column(db.Integer)


class CountryCountryYear(CountryXYear):

    __tablename__ = "country_country_year"


class CountryDepartmentYear(CountryXYear):

    __tablename__ = "country_department_year"


class CountryMSAYear(CountryXYear):

    __tablename__ = "country_msa_year"


class CountryMunicipalityYear(CountryXYear):

    __tablename__ = "country_municipality_year"


class PartnerProductYear(BaseModel, IDMixin):

    __tablename__ = "partner_product_year"

    country_id = db.Column(db.Integer, db.ForeignKey(Country.id))
    product_id = db.Column(db.Integer, db.ForeignKey(HSProduct.id))
    level = db.Column(product_enum)

    partner = db.relationship(Country)
    product = db.relationship(HSProduct)

    year = db.Column(db.Integer)

    export_value = db.Column(db.BIGINT)
    import_value = db.Column(db.BIGINT)
    export_num_plants = db.Column(db.Integer)
    import_num_plants = db.Column(db.Integer)


class DepartmentYear(BaseModel, IDMixin):

    __tablename__ = "department_year"

    location_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    year = db.Column(db.Integer)

    location = db.relationship(Location)

    eci = db.Column(db.Float)
    eci_rank = db.Column(db.Integer)
    diversity = db.Column(db.Float)
    coi = db.Column(db.Float)
    industry_coi = db.Column(db.Float)

    gdp_nominal = db.Column(db.BIGINT)
    gdp_real = db.Column(db.BIGINT)
    gdp_pc_nominal = db.Column(db.Integer)
    gdp_pc_real = db.Column(db.Integer)

    population = db.Column(db.Integer)

    employment = db.Column(db.Integer)
    wages = db.Column(db.BIGINT)
    monthly_wages = db.Column(db.Integer)
    num_establishments = db.Column(db.Integer)

    industry_eci = db.Column(db.Float)

    average_livestock_load = db.Column(db.Float)

    yield_index = db.Column(db.Float)


class MSAYear(BaseModel, IDMixin):

    __tablename__ = "msa_year"

    location_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    year = db.Column(db.Integer)

    location = db.relationship(Location)

    eci = db.Column(db.Float)
    coi = db.Column(db.Float)
    industry_coi = db.Column(db.Float)

    employment = db.Column(db.Integer)
    wages = db.Column(db.BIGINT)
    monthly_wages = db.Column(db.Integer)
    num_establishments = db.Column(db.Integer)
    industry_eci = db.Column(db.Float)


class MunicipalityYear(BaseModel, IDMixin):

    __tablename__ = "municipality_year"

    location_id = db.Column(db.Integer, db.ForeignKey(Location.id))
    year = db.Column(db.Integer)

    location = db.relationship(Location)

    average_livestock_load = db.Column(db.Float)

    yield_index = db.Column(db.Float)

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


class XLivestockYear(BaseModel, IDMixin):

    __abstract__ = True

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def livestock_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Livestock.id))

    livestock_level = db.Column(livestock_enum)

    num_livestock = db.Column(db.Integer)
    num_farms = db.Column(db.Integer)
    average_livestock_load = db.Column(db.Float)


class CountryLivestockYear(XLivestockYear):
    __tablename__ = "country_livestock_year"

class DepartmentLivestockYear(XLivestockYear):
    __tablename__ = "department_livestock_year"

class MunicipalityLivestockYear(XLivestockYear):
    __tablename__ = "municipality_livestock_year"


class XAgriculturalProductYear(BaseModel, IDMixin):

    __abstract__ = True

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def agproduct_id(cls):
        return db.Column(db.Integer, db.ForeignKey(AgriculturalProduct.id))

    agproduct_level = db.Column(agproduct_enum)
    year = db.Column(db.Integer)

    land_sown = db.Column(db.Integer)
    land_harvested = db.Column(db.Integer)
    production_tons = db.Column(db.Integer)
    yield_ratio = db.Column(db.Float)
    yield_index = db.Column(db.Float)


class CountryAgriculturalProductYear(XAgriculturalProductYear):
    __tablename__ = "country_agproduct_year"

class DepartmentAgriculturalProductYear(XAgriculturalProductYear):
    __tablename__ = "department_agproduct_year"

class MunicipalityAgriculturalProductYear(XAgriculturalProductYear):
    __tablename__ = "municipality_agproduct_year"


class XNonagYear(BaseModel, IDMixin):

    __abstract__ = True

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def nonag_id(cls):
        return db.Column(db.Integer, db.ForeignKey(NonagriculturalActivity.id))

    nonag_level = db.Column(nonag_enum)

    num_farms_ag = db.Column(db.Integer)
    num_farms_nonag = db.Column(db.Integer)
    num_farms = db.Column(db.Integer)


class CountryNonagYear(XNonagYear):
    __tablename__ = "country_nonag_year"

class DepartmentNonagYear(XNonagYear):
    __tablename__ = "department_nonag_year"

class MunicipalityNonagYear(XNonagYear):
    __tablename__ = "municipality_nonag_year"



class XLandUseYear(BaseModel, IDMixin):
    __abstract__ = True

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def land_use_id(cls):
        return db.Column(db.Integer, db.ForeignKey(LandUse.id))

    land_use_level = db.Column(land_use_enum)

    area = db.Column(db.Integer)


class CountryLandUseYear(XLandUseYear):
    __tablename__ = "country_land_use_year"

class DepartmentLandUseYear(XLandUseYear):
    __tablename__ = "department_land_use_year"

class MunicipalityLandUseYear(XLandUseYear):
    __tablename__ = "municipality_land_use_year"


class XFarmTypeYear(BaseModel, IDMixin):
    __abstract__ = True

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def farmtype_id(cls):
        return db.Column(db.Integer, db.ForeignKey(FarmType.id))

    farmtype_level = db.Column(farmtype_enum)

    num_farms = db.Column(db.Integer)


class CountryFarmTypeYear(XFarmTypeYear):
    __tablename__ = "country_farmtype_year"

class DepartmentFarmTypeYear(XFarmTypeYear):
    __tablename__ = "department_farmtype_year"

class MunicipalityFarmTypeYear(XFarmTypeYear):
    __tablename__ = "municipality_farmtype_year"


class XFarmSizeYear(BaseModel, IDMixin):
    __abstract__ = True

    @declared_attr
    def location_id(cls):
        return db.Column(db.Integer, db.ForeignKey(Location.id))

    @declared_attr
    def farmsize_id(cls):
        return db.Column(db.Integer, db.ForeignKey(FarmSize.id))

    farmsize_level = db.Column(farmsize_enum)

    avg_farmsize = db.Column(db.Integer)


class CountryFarmSizeYear(XFarmSizeYear):
    __tablename__ = "country_farmsize_year"

class DepartmentFarmSizeYear(XFarmSizeYear):
    __tablename__ = "department_farmsize_year"

class MunicipalityFarmSizeYear(XFarmSizeYear):
    __tablename__ = "municipality_farmsize_year"


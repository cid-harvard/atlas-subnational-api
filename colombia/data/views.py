from flask import Blueprint, request
from .models import (DepartmentProductYear, MunicipalityProductYear,
                     DepartmentIndustryYear, MunicipalityIndustryYear,
                     ProductYear, IndustryYear, DepartmentYear,
                     CountryMunicipalityProductYear,
                     CountryDepartmentProductYear)
from ..api_schemas import marshal
from .routing import lookup_classification_level
from .. import api_schemas as schemas

from ..core import db
from atlas_core.helpers.flask import abort

data_app = Blueprint("data", __name__)


def get_level():
    """Shortcut to get the ?level= query param"""
    level = request.args.get("level", None)
    if level is None:
        raise abort(400, body="Must specify ?level=")
    return level


def get_or_fail(name, dictionary):
    """Lookup a key in a dict, abort with helpful error on failure."""
    thing = dictionary.get(name, None)
    if thing is None:
        msg = "{} is not valid. Try one of: {}"\
            .format(
                thing,
                list(dictionary.keys()))
        raise abort(400, body=msg)
    return thing


entity_year = {
    "industry": {
        "model": IndustryYear,
        "schema": schemas.industry_year
    },
    "product": {
        "model": ProductYear,
        "schema": schemas.product_year
    }
}

entity_year_location = {
    "department": {
        "model": DepartmentYear,
        "schema": schemas.department_year
    }
    #"municipality" ...
}


def eey_product_exporters(entity_type, entity_id, location_level):

    if location_level == "department":
        q = DepartmentProductYear.query\
            .filter_by(product_id=entity_id)\
            .all()
        return marshal(schemas.department_product_year, q)
    elif location_level == "municipality":
        q = MunicipalityProductYear.query\
            .filter_by(product_id=entity_id)\
            .all()
        return marshal(schemas.municipality_product_year, q)
    else:
        msg = "Data doesn't exist at location level {}"\
            .format(location_level)
        abort(400, body=msg)


def eeey_location_products(entity_type, entity_id, buildingblock_level,
                           sub_id):

    if buildingblock_level != "country":
        msg = "Data doesn't exist at level {}. Try country.".format(buildingblock_level)
        abort(400, body=msg)

    # Assert level of sub_id is same as entity_id
    location_level = lookup_classification_level("location", entity_id)

    if location_level == "municipality":
        q = CountryMunicipalityProductYear.query\
            .filter_by(municipality_id=entity_id)\
            .filter_by(product_id=sub_id)\
            .all()
        return marshal(schemas.country_municipality_product_year, q)
    elif location_level == "department":
        q = CountryDepartmentProductYear.query\
            .filter_by(department_id=entity_id)\
            .filter_by(product_id=sub_id)\
            .all()
        return marshal(schemas.country_department_product_year, q)
    else:
        msg = "Data doesn't exist at location level {}"\
            .format(location_level)
        abort(400, body=msg)


def eey_industry_participants(entity_type, entity_id, location_level):

    if location_level == "department":
        q = DepartmentIndustryYear.query\
            .filter_by(industry_id=entity_id)\
            .all()
        return marshal(schemas.department_industry_year, q)
    elif location_level == "municipality":
        q = MunicipalityIndustryYear.query\
            .filter_by(industry_id=entity_id)\
            .all()
        return marshal(schemas.municipality_industry_year, q)
    else:
        msg = "Data doesn't exist at location level {}"\
            .format(location_level)
        abort(400, body=msg)


def eey_location_products(entity_type, entity_id, buildingblock_level):

    location_level = lookup_classification_level("location", entity_id)

    if location_level == "department":
        q = DepartmentProductYear.query\
            .filter_by(department_id=entity_id)\
            .filter_by(level=buildingblock_level)\
            .all()
        return marshal(schemas.department_product_year, q)
    elif location_level == "municipality":
        q = MunicipalityProductYear.query\
            .filter_by(municipality_id=entity_id)\
            .filter_by(level=buildingblock_level)\
            .all()
        return marshal(schemas.municipality_product_year, q)
    else:
        msg = "Data doesn't exist at location level {}"\
            .format(location_level)
        abort(400, body=msg)


def eey_location_industries(entity_type, entity_id, buildingblock_level):

    location_level = lookup_classification_level("location", entity_id)

    if location_level == "department":
        q = DepartmentIndustryYear.query\
            .filter_by(department_id=entity_id)\
            .filter_by(level=buildingblock_level)\
            .all()
        return marshal(schemas.department_industry_year, q)
    elif location_level == "municipality":
        q = MunicipalityIndustryYear.query\
            .filter_by(municipality_id=entity_id)\
            .filter_by(level=buildingblock_level)\
            .all()
        return marshal(schemas.municipality_industry_year, q)
    else:
        msg = "Data doesn't exist at location level {}"\
            .format(location_level)
        abort(400, body=msg)


entity_entity_year = {
    "industry": {
        "subdatasets": {
            "participants": {
                "func": eey_industry_participants
            }
        }
    },
    "product": {
        "subdatasets": {
            "exporters": {
                "func": eey_product_exporters
            }
        }
    },
    "location": {
        "subdatasets": {
            "products": {
                "func": eey_location_products,
                "sub_func": eeey_location_products
            },
            "industries": {
                "func": eey_location_industries
            }
        }
    },
}


@data_app.route("/<string:entity_type>/")
def entity_year_handler(entity_type):

    level = get_level()
    entity = get_or_fail(entity_type, entity_year)

    q = db.session\
        .query(entity["model"])\
        .filter_by(level=level)\
        .all()
    return marshal(entity["schema"], q)


@data_app.route("/location/")
def entity_year_handler_location():

    level = get_level()
    entity_config = get_or_fail(level, entity_year_location)

    q = db.session\
        .query(entity_config["model"])\
        .all()
    return marshal(entity_config["schema"], q)


@data_app.route("/<string:entity_type>/<int:entity_id>/<string:subdataset>/")
def entity_entity_year_handler(entity_type, entity_id, subdataset):

    buildingblock_level = get_level()
    entity_config = get_or_fail(entity_type, entity_entity_year)
    subdataset_config = get_or_fail(subdataset, entity_config["subdatasets"])

    return subdataset_config["func"](entity_type, entity_id, buildingblock_level)


@data_app.route("/<string:entity_type>/<int:entity_id>/<string:subdataset>/<int:sub_id>/")
def entity_entity_entity_year_handler(entity_type, entity_id, subdataset, sub_id):

    buildingblock_level = get_level()
    entity_config = get_or_fail(entity_type, entity_entity_year)
    subdataset_config = get_or_fail(subdataset, entity_config["subdatasets"])

    return subdataset_config["sub_func"](entity_type, entity_id, buildingblock_level, sub_id)

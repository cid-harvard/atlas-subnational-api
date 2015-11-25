from flask import Blueprint, request, jsonify
from .models import (CountryProductYear, DepartmentProductYear, MSAProductYear,
                     MunicipalityProductYear, DepartmentIndustryYear,
                     CountryIndustryYear, MSAIndustryYear,
                     MunicipalityIndustryYear, ProductYear, IndustryYear,
                     DepartmentYear, Location, CountryMunicipalityProductYear,
                     CountryDepartmentProductYear, OccupationYear,
                     OccupationIndustryYear, CountryCountryYear,
                     CountryDepartmentYear, CountryMSAYear,
                     CountryMunicipalityYear, MSAYear)
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
                name,
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
    },
    "occupation": {
        "model": OccupationYear,
        "schema": schemas.occupation_year
    }
}

entity_year_location = {
    "department": {
        "model": DepartmentYear,
        "schema": schemas.department_year
    },
    "msa": {
        "model": MSAYear,
        "schema": schemas.msa_year
    }
}

product_year_region_mapping = {
    "department": {"model": DepartmentProductYear},
    "msa": {"model": MSAProductYear},
    "municipality": {"model": MunicipalityProductYear},
    "country": {"model": CountryProductYear},
}

industry_year_region_mapping = {
    "department": {"model": DepartmentIndustryYear},
    "msa": {"model": MSAIndustryYear},
    "municipality": {"model": MunicipalityIndustryYear},
    "country": {"model": CountryIndustryYear},
}


def eey_product_exporters(entity_type, entity_id, location_level):

    if location_level in product_year_region_mapping:
        q = product_year_region_mapping[location_level]["model"].query\
            .filter_by(product_id=entity_id)\
            .all()
        schema = schemas.XProductYearSchema(many=True)
        schema.context = {'id_field_name': location_level + '_id'}
        return marshal(schema, q)
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
            .filter_by(location_id=entity_id)\
            .filter_by(product_id=sub_id)\
            .all()
        return marshal(schemas.country_municipality_product_year, q)
    elif location_level == "department":
        q = CountryDepartmentProductYear.query\
            .filter_by(location_id=entity_id)\
            .filter_by(product_id=sub_id)\
            .all()
        return marshal(schemas.country_department_product_year, q)
    else:
        msg = "Data doesn't exist at location level {}"\
            .format(location_level)
        abort(400, body=msg)


def eey_industry_participants(entity_type, entity_id, location_level):

    if location_level in product_year_region_mapping:

        q = industry_year_region_mapping[location_level]["model"].query\
            .filter_by(industry_id=entity_id)\
            .all()
        schema = schemas.XIndustryYearSchema(many=True)
        schema.context = {'id_field_name': location_level + '_id'}
        return marshal(schema, q)
    else:
        msg = "Data doesn't exist at location level {}"\
            .format(location_level)
        abort(400, body=msg)


def eey_location_products(entity_type, entity_id, buildingblock_level):

    location_level = lookup_classification_level("location", entity_id)

    if location_level in product_year_region_mapping:
        query_model = product_year_region_mapping[location_level]["model"]
        q = query_model.query\
            .filter_by(level=buildingblock_level)\

        if hasattr(query_model, "location_id"):
            q = q.filter_by(location_id=entity_id)

        schema = schemas.XProductYearSchema(many=True)
        schema.context = {'id_field_name': location_level + '_id'}
        return marshal(schema, q)
    else:
        msg = "Data doesn't exist at location level {}"\
            .format(location_level)
        abort(400, body=msg)


def eey_location_industries(entity_type, entity_id, buildingblock_level):

    location_level = lookup_classification_level("location", entity_id)

    if location_level in industry_year_region_mapping:
        query_model = industry_year_region_mapping[location_level]["model"]
        q = query_model.query\
            .filter_by(level=buildingblock_level)\

        if hasattr(query_model, "location_id"):
            q = q.filter_by(location_id=entity_id)

        schema = schemas.XIndustryYearSchema(many=True)
        schema.context = {'id_field_name': location_level + '_id'}
        return marshal(schema, q)


def eey_location_subregions_trade(entity_type, entity_id, buildingblock_level):

    location_level = lookup_classification_level("location", entity_id)

    if location_level == "country" and buildingblock_level == "department":
        model = DepartmentProductYear
    elif location_level == "department" and buildingblock_level == "municipality":
        model = MunicipalityProductYear
    else:
        return jsonify(data=[])

    subregions = db.session\
        .query(Location.id)\
        .filter_by(parent_id=entity_id)\
        .subquery()

    q = db.session.query(
        db.func.sum(model.export_value).label("export_value"),
        db.func.sum(model.export_num_plants).label("export_num_plants"),
        db.func.sum(model.import_value).label("import_value"),
        db.func.sum(model.import_num_plants).label("import_num_plants"),
        model.location_id,
        model.year,
    )\
        .filter(model.location_id.in_(subregions))\
        .group_by(model.location_id, model.year)
    return jsonify(data=[x._asdict() for x in q])


def eey_location_partners(entity_type, entity_id, buildingblock_level):

    if buildingblock_level != "country":
        msg = "Data doesn't exist at level {}. Try country.".format(buildingblock_level)
        abort(400, body=msg)

    # Assert level of sub_id is same as entity_id
    location_level = lookup_classification_level("location", entity_id)

    if location_level == "department":
        q = CountryDepartmentYear.query\
            .filter_by(location_id=entity_id)\
            .all()
        return marshal(schemas.country_x_year, q)
    elif location_level == "msa":
        q = CountryMSAYear.query\
            .filter_by(location_id=entity_id)\
            .all()
        return marshal(schemas.country_x_year, q)
    elif location_level == "country":
        q = CountryCountryYear.query\
            .filter_by(location_id=entity_id)\
            .all()
        return marshal(schemas.country_x_year, q)
    elif location_level == "municipality":
        q = CountryMunicipalityYear.query\
            .filter_by(location_id=entity_id)\
            .all()
        return marshal(schemas.country_x_year, q)
    else:
        msg = "Data doesn't exist at location level {}"\
            .format(location_level)
        abort(400, body=msg)


def eey_industry_occupations(entity_type, entity_id, buildingblock_level):

    if buildingblock_level != "minor_group":
        msg = "Data doesn't exist at building block level {}"\
            .format(buildingblock_level)
        abort(400, body=msg)

    q = OccupationIndustryYear.query\
        .filter_by(industry_id=entity_id)\
        .all()
    return marshal(schemas.occupation_year, q)


entity_entity_year = {
    "industry": {
        "subdatasets": {
            "participants": {
                "func": eey_industry_participants
            },
            "occupations": {
                "func": eey_industry_occupations
            },
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
            },
            "subregions_trade": {
                "func": eey_location_subregions_trade
            },
            "partners": {
                "func": eey_location_partners
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

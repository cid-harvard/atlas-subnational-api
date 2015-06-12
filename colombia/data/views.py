from flask import Blueprint, request
from .models import (DepartmentProductYear, DepartmentIndustryYear,
                     ProductYear, Location)
from ..api_schemas import marshal
from .. import api_schemas as schemas

from ..core import db
from atlas_core.helpers.flask import abort, jsonify

products_app = Blueprint("products", __name__)
departments_app = Blueprint("departments", __name__)
industries_app = Blueprint("industries", __name__)


@products_app.route("/trade/departments/<int:department>/",
                    defaults={"year": None})
@products_app.route("/trade/departments/<int:department>/<int:year>")
def department_product_year(department, year):
    """Get the trades done by a department in a specific year or across all
    years.

    :param department: See :py:class:`colombia.models.Department` id
    :param year: 4-digit year
    """
    q = db.session\
        .query(
            DepartmentProductYear.import_value,
            DepartmentProductYear.export_value,
            DepartmentProductYear.export_rca,
            DepartmentProductYear.distance,
            DepartmentProductYear.cog,
            DepartmentProductYear.coi,
            DepartmentProductYear.department_id,
            DepartmentProductYear.product_id,
            DepartmentProductYear.year,
        )\
        .filter_by(department_id=int(department))

    if year is not None:
        q = q.filter_by(year=year)

    return marshal(schemas.department_product_year, q)


@products_app.route("/trade/products/<int:product>/",
                    defaults={"year": None})
@products_app.route("/trade/products/<int:product>/<int:year>")
def department_product_year_by_product(product, year):
    """Get the departments that traded a product in a specific year or
    across all years.

    :param product: See :py:class:`colombia.models.HSProduct` id
    :param year: 4-digit year
    """

    q = db.session\
        .query(
            DepartmentProductYear.import_value,
            DepartmentProductYear.export_value,
            DepartmentProductYear.export_rca,
            DepartmentProductYear.distance,
            DepartmentProductYear.cog,
            DepartmentProductYear.coi,
            DepartmentProductYear.department_id,
            DepartmentProductYear.product_id,
            DepartmentProductYear.year,
        )\
        .filter_by(product_id=int(product))

    if year is not None:
        q = q.filter_by(year=year)

    return marshal(schemas.department_product_year, q)


@products_app.route("/trade/metadata/",
                    defaults={"year": None})
@products_app.route("/trade/metadata/<int:year>")
def product_year(year):
    """Get product / year specific variables (e.g. product complexity) in a
    specific year or across all years.
    """
    q = ProductYear.query
    if year is not None:
        q = q.filter_by(year=year)

    return marshal(schemas.product_year, q)


@products_app.route("/products")
@products_app.route("/products/<int:product_id>")
def products_index(product_id=None):

    location_id = request.args.get("location", None)
    year = request.args.get("year", None)

    # Find type of location
    if location_id:
        location_type = Location.query.get_or_404(location_id).level

    if location_id is not None and year is not None:
        if location_type == "department":
            q = DepartmentProductYear.query\
                .filter_by(year=year, department_id=location_id)
            return marshal(schemas.department_product_year, q)
    elif location_id is not None:
        if location_type == "department":
            q = DepartmentProductYear.query\
                .filter_by(department_id=location_id)
            return marshal(schemas.department_product_year, q)

    raise abort(400, body="Could not find data with the given parameters.")


@departments_app.route("/departments")
@departments_app.route("/departments/<int:department_id>")
def departments_index(department_id=None):

    year = request.args.get("year", None)

    if year is not None:
        dpy = DepartmentProductYear
        q = db.session\
            .query(dpy.department_id,
                   db.func.sum(dpy.export_value).label("export_value"))\
            .filter_by(year=year)\
            .group_by(dpy.department_id)\
            .all()

        return jsonify(data=[x._asdict() for x in q])

    raise abort(400, body="Could not find data with the given parameters.")


@industries_app.route("/industries")
@industries_app.route("/industries/<int:industry_id>")
def industries_index(product_id=None):

    location_id = request.args.get("location", None)
    year = request.args.get("year", None)

    # Find type of location
    if location_id:
        location_type = Location.query.get_or_404(location_id).level

    if location_id is not None and year is not None:
        if location_type == "department":
            q = DepartmentIndustryYear.query\
                .filter_by(year=year, department_id=location_id)
            return marshal(schemas.department_industry_year, q)
    elif location_id is not None:
        if location_type == "department":
            q = DepartmentIndustryYear.query\
                .filter_by(department_id=location_id)
            return marshal(schemas.department_industry_year, q)

    raise abort(400, body="Could not find data with the given parameters.")

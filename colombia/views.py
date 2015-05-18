from flask import request, Blueprint
from .models import (HSProduct, Department, DepartmentProductYear,
                             ProductYear)
from .api_schemas import marshal
from . import api_schemas as schemas

from .core import db


metadata_app = Blueprint("metadata", __name__)
products_app = Blueprint("products", __name__)


@metadata_app.route("/products/<int:product_id>")
def product(product_id):
    """Get a :py:class:`~colombia.models.HSProduct` with the given code.

    :param code: See :py:class:`colombia.models.HSProduct.code`
    :type code: int
    :code 404: product doesn't exist
    """
    q = HSProduct.query.filter_by(id=product_id)\
        .first_or_abort(product_id)
    return marshal(schemas.hs_product, q, many=False)


@metadata_app.route("/products/")
def products():
    """Get all the :py:class:`~colombia.models.HSProduct` s.

    :query aggregation:  Filter by
      :py:class:`colombia.models.HSProduct.aggregation` if specified.
    """

    aggregation = request.args.get("aggregation", None)
    q = HSProduct.query\
        .filter_by_enum(HSProduct.aggregation, aggregation)

    return marshal(schemas.hs_product, q)


@metadata_app.route("/departments/<int:department_id>")
def department(department_id):
    """Get a :py:class:`~colombia.models.Department` with the given code.

    :param code: See :py:class:`colombia.models.Department.code`
    :type code: int
    :code 404: department doesn't exist
    """
    q = Department.query.filter_by(id=department_id)\
        .first_or_abort(department_id)
    return marshal(schemas.department, q, many=False)


@metadata_app.route("/departments/")
def departments():
    """Get all the :py:class:`~colombia.models.Department` s."""
    q = Department.query.all()
    return marshal(schemas.department, q)


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

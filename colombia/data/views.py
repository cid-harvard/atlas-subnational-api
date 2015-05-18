from flask import Blueprint
from .models import (DepartmentProductYear, ProductYear)
from ..api_schemas import marshal
from .. import api_schemas as schemas

from ..core import db


products_app = Blueprint("products", __name__)


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

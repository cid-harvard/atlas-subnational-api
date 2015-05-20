from flask import request, Blueprint
from .models import (HSProduct, Department)
from ..api_schemas import marshal
from .. import api_schemas as schemas


metadata_app = Blueprint("metadata", __name__)


@metadata_app.route("/products/<int:product_id>")
def product(product_id):
    """Get a :py:class:`~colombia.models.HSProduct` with the given code.

    :param code: See :py:class:`colombia.models.HSProduct.code`
    :type code: int
    :code 404: product doesn't exist
    """
    q = HSProduct.query.filter_by(id=product_id)\
        .first_or_abort(product_id)
    return marshal(schemas.metadata, q, many=False)


@metadata_app.route("/products/")
def products():
    """Get all the :py:class:`~colombia.models.HSProduct` s.

    :query aggregation:  Filter by
      :py:class:`colombia.models.HSProduct.aggregation` if specified.
    """

    level = request.args.get("level", None)
    q = HSProduct.query\
        .filter_by_enum(HSProduct.level, level)

    return marshal(schemas.metadata, q)


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


from flask import request
from flask.ext import restful
from flask.ext.restful import fields, marshal_with, marshal
from colombia.models import HSProduct, Department, DepartmentProductYear


hs_product_fields = {
    'code': fields.String,
    'id': fields.String,
    'name': fields.String,
    'aggregation': fields.String,
}

department_fields = {
    'code': fields.String,
    'id': fields.String,
    'name': fields.String,
    'population': fields.Integer,
    'gdp': fields.Integer,
}

department_product_year_fields = {
    'import_value': fields.Integer,
    'export_value': fields.Integer,
    'export_rca': fields.Float,
    'distance': fields.Float,
    'opp_gain': fields.Float,

    'id': fields.String,
    'department_id': fields.String,
    'product_id': fields.String,
    'year': fields.Integer
}


def make_id_dictionary(items, id_field='id'):
    """Take a list of dicts (that contain an id field and a bunch of other
    stuff) and turn it into a dict of ids

        e.g. [{'id':3, 'value':7}, {'id':4, "value":1}] into:
        {3:{'value':7}, 4:{'value':1}}

    This is useful when I want to return a list of items as a javascript object
    instead of an array, to make life easier for frontend.

    :param id_field: Name of the dict key to use as id.
    """
    ret = {}
    for item in items:
        assert id_field in item, "Each element must have an id field"
        id_val = item.pop(id_field)
        ret[id_val] = item

    return ret


class marshal_as_dict(object):

    def __init__(self, schema, id_field='id'):
        self.schema = schema
        self.id_field = id_field

    def __call__(self, f):
        def inner(*args, **kwargs):
            invocation_result = f(*args, **kwargs)
            marshalled_data = marshal(
                invocation_result,
                self.schema
            )
            return (make_id_dictionary(marshalled_data, self.id_field),
                    200)

        return inner


class HSProductAPI(restful.Resource):

    @marshal_with(hs_product_fields)
    def get(self, code):
        """Get a :py:class:`~colombia.models.HSProduct` with the given code.

        :param code: See :py:class:`colombia.models.HSProduct.code`
        :type code: int
        :code 404: product doesn't exist
        """
        q = HSProduct.query.filter_by(code=code).first_or_abort(code)
        return q


class HSProductListAPI(restful.Resource):

    @marshal_as_dict(hs_product_fields)
    def get(self):
        """Get all the :py:class:`~colombia.models.HSProduct` s.

        :query aggregation:  Filter by
          :py:class:`colombia.models.HSProduct.aggregation` if specified.
        """

        aggregation = request.args.get("aggregation", None)
        q = HSProduct.query\
            .filter_by_enum(HSProduct.aggregation, aggregation)

        return q.all()


class DepartmentAPI(restful.Resource):

    @marshal_with(department_fields)
    def get(self, code):
        """Get a :py:class:`~colombia.models.Department` with the given code.

        :param code: See :py:class:`colombia.models.Department.code`
        :type code: int
        :code 404: department doesn't exist
        """
        q = Department.query.filter_by(code=code).first_or_abort(code)
        return q


class DepartmentListAPI(restful.Resource):

    @marshal_as_dict(department_fields)
    def get(self):
        """Get all the :py:class:`~colombia.models.Department` s."""
        return Department.query.all()


class DepartmentProductYearByDepartmentAPI(restful.Resource):

    @marshal_with(department_product_year_fields)
    def get(self, department, year):
        """Get the trades done by a department in a specific year or across all
        years.

        :param department: See :py:class:`colombia.models.Department` id
        :param year: 4-digit year
        """
        q = DepartmentProductYear.query.filter_by(department_id=int(department))
        if year is not None:
            q = q.filter_by(year=year)
        return q.all()


class DepartmentProductYearByProductAPI(restful.Resource):

    @marshal_with(department_product_year_fields)
    def get(self, product, year):
        """Get the departments that traded a product in a specific year or
        across all years.

        :param product: See :py:class:`colombia.models.HSProduct` id
        :param year: 4-digit year
        """
        q = DepartmentProductYear.query.filter_by(product_id=int(product))
        if year is not None:
            q = q.filter_by(year=year)
        return q.all()

from flask import request
from flask.ext import restful
from flask.ext.restful import fields, marshal_with
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

    'id': fields.Integer,
    'department_id': fields.Integer,
    'product_id': fields.Integer,
    'year': fields.Integer
}


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

    @marshal_with(hs_product_fields)
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

    @marshal_with(department_fields)
    def get(self):
        """Get all the :py:class:`~colombia.models.Department` s."""
        return Department.query.all()


class DepartmentProductYearAPI(restful.Resource):

    @marshal_with(department_product_year_fields)
    def get(self, department, year):
        """Get the trades done by a department in a specific year or across all
        years.

        :param department: See :py:class:`colombia.models.Department` id
        :param year: 4-digit year
        """
        q = DepartmentProductYear.query.filter_by(department_id=department)
        if year is not None:
            q = q.filter_by(year=year)
        print(str(q))
        return q.all()

from flask import request, jsonify
from flask.ext import restful
from flask.ext.restful import marshal_with
from colombia.models import (HSProduct, Department, DepartmentProductYear,
                             ProductYear)
from colombia.api_schemas import marshal
import colombia.api_schemas as schemas

from functools import wraps

from atlas_core import db



class HSProductAPI(restful.Resource):

    def get(self, product_id):
        """Get a :py:class:`~colombia.models.HSProduct` with the given code.

        :param code: See :py:class:`colombia.models.HSProduct.code`
        :type code: int
        :code 404: product doesn't exist
        """
        q = HSProduct.query.filter_by(id=product_id).first_or_abort(product_id)
        return marshal(schemas.hs_product, q, many=False)


class HSProductListAPI(restful.Resource):

    def get(self):
        """Get all the :py:class:`~colombia.models.HSProduct` s.

        :query aggregation:  Filter by
          :py:class:`colombia.models.HSProduct.aggregation` if specified.
        """

        aggregation = request.args.get("aggregation", None)
        q = HSProduct.query\
            .filter_by_enum(HSProduct.aggregation, aggregation)

        return marshal(schemas.hs_product, q)


class DepartmentAPI(restful.Resource):

    def get(self, department_id):
        """Get a :py:class:`~colombia.models.Department` with the given code.

        :param code: See :py:class:`colombia.models.Department.code`
        :type code: int
        :code 404: department doesn't exist
        """
        q = Department.query.filter_by(id=department_id).first_or_abort(department_id)
        return marshal(schemas.department, q, many=False)


class DepartmentListAPI(restful.Resource):

    def get(self):
        """Get all the :py:class:`~colombia.models.Department` s."""
        q = Department.query.all()
        return marshal(schemas.department, q)


class DepartmentProductYearByDepartmentAPI(restful.Resource):

    def get(self, department, year):
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


class DepartmentProductYearByProductAPI(restful.Resource):

    def get(self, product, year):
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


class ProductYearAPI(restful.Resource):

    def get(self, year):
        """Get product / year specific variables (e.g. product complexity) in a
        specific year or across all years.
        """
        q = ProductYear.query
        if year is not None:
            q = q.filter_by(year=year)

        return marshal(schemas.product_year, q)

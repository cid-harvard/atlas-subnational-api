from flask import request, jsonify
from flask.ext import restful
from flask.ext.restful import fields, marshal_with, marshal
from colombia.models import (HSProduct, Department, DepartmentProductYear,
                             ProductYear)
from colombia.api_schemas import (hs_product_fields, department_fields,
                                  department_product_year_fields, product_year_fields)
import colombia.api_schemas as schemas

from functools import wraps

from atlas_core import db

class HSProductAPI(restful.Resource):

    @marshal_with(hs_product_fields)
    def get(self, product_id):
        """Get a :py:class:`~colombia.models.HSProduct` with the given code.

        :param code: See :py:class:`colombia.models.HSProduct.code`
        :type code: int
        :code 404: product doesn't exist
        """
        q = HSProduct.query.filter_by(id=product_id).first_or_abort(product_id)
        return q


class HSProductListAPI(restful.Resource):

    #@headers({"Cache-Control": "max-age=600"})
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
    def get(self, department_id):
        """Get a :py:class:`~colombia.models.Department` with the given code.

        :param code: See :py:class:`colombia.models.Department.code`
        :type code: int
        :code 404: department doesn't exist
        """
        q = Department.query.filter_by(id=department_id).first_or_abort(department_id)
        return q


class DepartmentListAPI(restful.Resource):

    #@headers({"Cache-Control": "max-age=600"})
    @marshal_with(department_fields)
    def get(self):
        """Get all the :py:class:`~colombia.models.Department` s."""
        return Department.query.all()


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
            .filter_by(department_id=int(department))\
            .filter(
                (DepartmentProductYear.export_value > 0)\
                | (DepartmentProductYear.import_value > 0))
        if year is not None:
            q = q.filter_by(year=year)
        return jsonify(data=schemas.department_product_year.dump(q).data)


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
        return jsonify(data=schemas.department_product_year.dump(q).data)


class ProductYearAPI(restful.Resource):

    @marshal_with(product_year_fields)
    def get(self, year):
        """Get product / year specific variables (e.g. product complexity) in a
        specific year or across all years.
        """
        q = ProductYear.query
        if year is not None:
            q = q.filter_by(year=year)
        return q.all()

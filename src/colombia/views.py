from flask.ext import restful
from flask.ext.restful import fields, marshal_with
from colombia.models import HSProduct
from colombia import ext


hs_product_fields = {
    'id': fields.String,
    'born_at': fields.Integer,
    'name': fields.String,
}


class HSProductAPI(restful.Resource):

    @ext.cache.cached(timeout=60)
    @marshal_with(hs_product_fields)
    def get(self, code):
        """Get a :py:class:`~colombia.models.HSProduct` with the given code.

        :param code: See :py:class:`colombia.models.HSProduct.code`
        :type code: int
        :code 404: product doesn't exist

        """

        q = HSProduct.query.filter_by(code=code).first_or_abort(code)

        return q

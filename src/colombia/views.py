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
    def get(self, cat_id):
        """Get a :py:class:`~colombia.models.Cat` with the given cat ID.

        :param id: unique ID of the cat
        :type id: int
        :code 404: cat doesn't exist

        """

        q = HSProduct.query.get_or_404(cat_id)

        return q

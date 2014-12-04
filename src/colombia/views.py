from flask.ext import restful
from flask.ext.restful import fields, marshal_with
from colombia.models import Cat
from colombia.ext import cache


cat_fields = {
    'id': fields.String,
    'born_at': fields.Integer,
    'name': fields.String,
}


class CatAPI(restful.Resource):

    @cache.cached(timeout=60)
    @marshal_with(cat_fields)
    def get(self, cat_id):
        """Get a :py:class:`~colombia.models.Cat` with the given cat ID.

        :param id: unique ID of the cat
        :type id: int
        :code 404: cat doesn't exist

        """

        q = Cat.query.get_or_404(cat_id)

        return q

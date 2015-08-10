from flask import request, Blueprint, jsonify
from .models import HSProduct, Industry
from ..api_schemas import marshal
from .. import api_schemas as schemas
from ..core import db

from ..entities import metadata_apis

from atlas_core.helpers.flask import abort
from sqlalchemy.orm import aliased


def make_metadata_api(metadata_class):
    """Since all metadata APIs look very similar, this function just generates
    the function that'll handle the API endpoint for an entity. It generates a
    function that handles both /metadata/entity/ and /metadata/entity/<id>."""

    def metadata_api(entity_id):
        """Get all :py:class:`~colombia.models.Metadata` s or a single one with the
        given id.

        :param id: Entity id, see :py:class:`colombia.models.Metadata.id`
        :type id: int
        :code 404: Entity doesn't exist
        """
        q = metadata_class.query

        if entity_id is not None:
            q = q.get_or_abort(entity_id)
            return marshal(schemas.metadata, q, many=False)
        else:
            level = request.args.get("level", None)
            q = q.filter_by_enum(metadata_class.level, level)
            return marshal(schemas.metadata, q)
    return metadata_api


def register_metadata_apis(metadata_apis):
    """Given an entity class, generate an API handler and register URL routes
    with flask. """

    for entity_name, settings in metadata_apis.items():

        # Generate handler function for entity
        api_func = make_metadata_api(settings["entity_model"])

        # Singular endpoint e.g. /entity/7
        metadata_app.add_url_rule(
            "/{entity_name}/<int:entity_id>".format(
                entity_name=settings["plural"]),
            endpoint=entity_name,
            view_func=api_func)

        # List endpoint e.g. /entity/
        metadata_app.add_url_rule(
            "/{entity_name}/".format(entity_name=settings["plural"]),
            endpoint=entity_name,
            view_func=api_func,
            defaults={"entity_id": None})

        settings["api"] = api_func


metadata_app = Blueprint("metadata", __name__)
register_metadata_apis(metadata_apis)


@metadata_app.route("/<string:entity_name>/hierarchy")
def hierarchy(entity_name):

    from_level = request.args.get("from_level", None)
    to_level = request.args.get("to_level", None)

    if entity_name == "products":
        if from_level == "4digit" and to_level == "section":
            p, p2, p3 = HSProduct, aliased(HSProduct), aliased(HSProduct)
            q = db.session.query(p.id, p3.id)\
                .join(p2, p2.id == p.parent_id)\
                .join(p3, p3.id == p2.parent_id)\
                .all()
            return jsonify(data=dict(q))
    elif entity_name == "industries":
        if from_level == "4digit" and to_level == "section":
            i, i2, i3 = Industry, aliased(Industry), aliased(Industry)
            q = db.session.query(i.id, i3.id)\
                .join(i2, i2.id == i.parent_id)\
                .join(i3, i3.id == i2.parent_id)\
                .all()
            return jsonify(data=dict(q))

    raise abort(400, body="""This API is still a fixture, try
                ?from_level=4digit&to_level=section.""")

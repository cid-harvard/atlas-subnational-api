from flask import request, Blueprint
from .models import (HSProduct, Location)
from ..api_schemas import marshal
from .. import api_schemas as schemas


metadata_app = Blueprint("metadata", __name__)


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

        if entity_id:
            q = q.get_or_abort(entity_id)
            return marshal(schemas.metadata, q, many=False)
        else:
            level = request.args.get("level", None)
            q = q.filter_by_enum(metadata_class.level, level)
            return marshal(schemas.metadata, q)
    return metadata_api


def register_metadata_apis():
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


metadata_apis = {
    "product": {
        "entity_model": HSProduct,
        "plural": "products",
    },
    "location": {
        "entity_model": Location,
        "plural": "locations",
    },
}

register_metadata_apis()

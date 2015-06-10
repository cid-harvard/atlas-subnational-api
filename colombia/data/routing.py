from ..entities import entities, metadata_apis
from .. import models

from collections import defaultdict
import re

from flask import request

RANGE_RE = r"^(from|to)_(.+)$"


def extract_route_params(request):
    """Given a flask request object, look at the query string and extract
    entity name / id pairs and handle things like from_location /
    to_location."""

    params = {}
    range_params = defaultdict(dict)
    for entity_name, value in request.args.items():

        # Check for from_foo, to_foo param pairs
        range_match = re.match(RANGE_RE, entity_name)
        if range_match is not None:
            range_part, entity_name = range_match.groups()
            if entity_name not in entities:
                raise ValueError("""Parameter '{}' is not a valid entity. Expected:
                                 {}""".format(entity_name, entities.keys()))
            range_params[entity_name][range_part] = int(value)
        # Check for regular entities
        elif entity_name in entities:
            params[entity_name] = int(value)
        else:
            raise ValueError("""Parameter '{}' is not a valid entity. Expected:
                             {}""".format(entity_name, entities.keys()))

    # Check for from_ without to_ and vice versa
    for key, value in range_params.items():
        if "from" not in value or "to" not in value:
            raise ValueError("""Range parameters error with {}: must contain
                             both from_foo and to_foo!""".format(key))

    params.update(range_params)
    return params


def lookup_classification_level(entity_type, entity_id):
    """Example: is this product_id a 2digit or 4digit product? Is this location
    id a department or a municipality?"""
    entity_class = metadata_apis[entity_type]["entity_model"]
    assert issubclass(entity_class, models.Metadata)
    return entity_class.query.get_or_404(entity_id).level


def make_entity_endpoint(route):
    """Autogenerates a flask endpoint that'll handle querystring-based
    routing."""
    def entity_endpoint(main_entity_name, main_entity_id):

        # Extract params from URL query string
        route_params = extract_route_params(request)

        # Find which classification level is being used for each entity
        route_key = {}
        for entity_name, entity_id in route_params.items():

            if entity_name == "year":
                entity_level = None
            else:
                entity_level = lookup_classification_level(entity_name, entity_id)

            route_key[entity_name] = entity_level

        route_key = tuple(sorted(route_key.items(), key=lambda x: x[0]))

        entity_config = route[main_entity_name]
        current_route = entity_config[route_key]

        return current_route["action"](**route_params)
    return entity_endpoint


def add_routes(app, route):
    """Add an entity handling route to a flask app / blueprint."""

    # Sort route keys to make sure order doesn't matter and they always match
    sorted_route = {}
    for entity_name, entity_route in route.items():
        sorted_route[entity_name] = {}
        for route_key, route_config in entity_route.items():
            route_key = tuple(sorted(route_key, key=lambda x: x[0]))
            sorted_route[entity_name][route_key] = route_config

    possible_entity_strings = ",".join("'" + key + "'" for key in route.keys())

    url_rule = "/<any({}):main_entity_name>".format(possible_entity_strings)
    app.add_url_rule(url_rule, "entity_handler_many",
                     make_entity_endpoint(sorted_route), methods=["GET"], defaults={"main_entity_id": None})
    url_rule = "/<any({}):main_entity_name>/<int:main_entity_id>".format(possible_entity_strings)
    app.add_url_rule(url_rule, "entity_handler_individual",
                     make_entity_endpoint(sorted_route), methods=["GET"])
    return app

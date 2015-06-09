from ..entities import entities, metadata_apis
from .. import models

from collections import defaultdict
import re

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


def make_entity_endpoint(route):
    def entity_endpoint(entity_name, entity_id):
        route_params = (("location", "department"), ("year", None))
        entity_config = route[entity_name]
        current_route = entity_config[route_params]
        return current_route["action"](location=2, department=7)
    return entity_endpoint


def add_routes(app, route):
    """Add an entity handling route to a flask app / blueprint."""
    url_rule = "/<any({}):entity_name>".format(",".join(route.keys()))
    app.add_url_rule(url_rule, "entity_handler_many",
                     make_entity_endpoint(route), methods=["GET"], defaults={"entity_id": None})
    url_rule = "/<any({}):entity_name>/<int:entity_id>".format(",".join(route.keys()))
    app.add_url_rule(url_rule, "entity_handler_individual",
                     make_entity_endpoint(route), methods=["GET"])
    return app

from ..entities import entities

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

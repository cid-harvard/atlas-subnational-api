from atlas_core import db, babel

from flask.ext.cache import Cache
from raven.contrib.flask import Sentry

cache = Cache()
sentry = Sentry()

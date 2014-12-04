from flask.ext.cache import Cache
from raven.contrib.flask import Sentry
from flask.ext import restful, restless
from flask.ext.sqlalchemy import SQLAlchemy

db = SQLAlchemy()
sentry = Sentry()
api = restful.Api()
cache = Cache()
restless_api = restless.APIManager()

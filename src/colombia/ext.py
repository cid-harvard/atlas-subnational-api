from flask.ext.cache import Cache
from raven.contrib.flask import Sentry
from flask.ext import restful
from flask.ext.sqlalchemy import SQLAlchemy

db = SQLAlchemy()
sentry = Sentry()
api = restful.Api()
cache = Cache()

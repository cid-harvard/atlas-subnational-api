from flask import Flask

from flask_debugtoolbar import DebugToolbarExtension
from werkzeug.contrib.profiler import ProfilerMiddleware

from flask.ext.cache import Cache
from raven.contrib.flask import Sentry
from flask.ext import restful
from flask.ext.sqlalchemy import SQLAlchemy


class ext(object):
    """Flask extensions."""

    db = SQLAlchemy()
    sentry = Sentry()
    api = restful.Api()
    cache = Cache()

    @classmethod
    def reset(cls):
        """To use in unittest teardowns - reset all extensions."""
        cls.db = SQLAlchemy()
        cls.sentry = Sentry()
        cls.api = restful.Api()
        cls.cache = Cache()



def create_app(config={}):
    app = Flask("colombia")
    app.config.from_envvar("FLASK_CONFIG")
    app.config.update(config)

    cache, db, api = ext.cache, ext.db, ext.api

    cache.init_app(app)
    db.init_app(app)

    # API Endpoints
    # Background on the URL scheme: https://github.com/cid-harvard/atlas-economic-complexity/issues/125
    from colombia.views import (HSProductAPI, HSProductListAPI, DepartmentAPI,
                                DepartmentListAPI,
                                DepartmentProductYearByDepartmentAPI,
                                DepartmentProductYearByProductAPI,
                                )
    api.add_resource(HSProductAPI, "/products/<int:product_id>", endpoint="product")
    api.add_resource(HSProductListAPI, "/products/", endpoint="products")
    api.add_resource(DepartmentAPI, "/departments/<int:department_id>",
                     endpoint="department")
    api.add_resource(DepartmentListAPI, "/departments/",
                     endpoint="departments")

    # Trades by Product
    api.add_resource(DepartmentProductYearByDepartmentAPI,
                     "/trade/departments/<int:department>/",
                     endpoint="department_product_by_department", defaults={"year": None})
    api.add_resource(DepartmentProductYearByDepartmentAPI,
                     "/trade/departments/<int:department>/<int:year>",
                     endpoint="department_product_year_by_department")

    # Trades by Department
    api.add_resource(DepartmentProductYearByProductAPI,
                     "/trade/products/<int:product>/",
                     endpoint="department_product_by_product", defaults={"year": None})
    api.add_resource(DepartmentProductYearByProductAPI,
                     "/trade/products/<int:product>/<int:year>",
                     endpoint="department_product_year_by_product")

    api.init_app(app)

    #from colombia.models import HSProduct
    #restless_api.init_app(app, flask_sqlalchemy_db=db)
    #restless_api.create_api(HSProduct, ['GET'], exclude_columns=["en", "es"],
    #                        results_per_page=-1)

    # CORS hook for debug reasons.
    @app.after_request
    def cors(response):
        if app.debug:
            response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    with app.app_context():
        db.create_all()

    # Debug tools
    if app.debug:
        DebugToolbarExtension(app)
        if app.config.get("PROFILE", False):
            app.wsgi_app = ProfilerMiddleware(app.wsgi_app,
                                              restrictions=[30],
                                              sort_by=("time", "cumulative"))

    return app

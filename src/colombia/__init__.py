from flask.ext.cache import Cache
from raven.contrib.flask import Sentry
from flask.ext import restful

import atlas_core


class ext(object):
    """Flask extensions."""

    sentry = Sentry()
    api = restful.Api()
    cache = Cache()

    @classmethod
    def reset(cls):
        """To use in unittest teardowns - reset all extensions."""
        cls.sentry = Sentry()
        cls.api = restful.Api()
        cls.cache = Cache()


def create_app(config={}):

    # Create base app from atlas_core
    app = atlas_core.create_app(additional_config=config, name="colombia")
    cache, api = ext.cache, ext.api

    cache.init_app(app)

    # API Endpoints
    # Background on the URL scheme: https://github.com/cid-harvard/atlas-economic-complexity/issues/125
    from colombia.views import (HSProductAPI, HSProductListAPI, DepartmentAPI,
                                DepartmentListAPI,
                                DepartmentProductYearByDepartmentAPI,
                                DepartmentProductYearByProductAPI,
                                ProductYearAPI,
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

    # Product / Year variables
    api.add_resource(ProductYearAPI,
                     "/trade/metadata/",
                     endpoint="product_year", defaults={"year": None})
    api.add_resource(ProductYearAPI,
                     "/trade/metadata/<int:year>",
                     endpoint="product_year_by_year")

    api.init_app(app)


    # CORS hook for debug reasons.
    @app.after_request
    def cors(response):
        if app.debug:
            response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    with app.app_context():
        # Register sqlalchemy model base so that models in this project that
        # use it are also registered
        atlas_core.db.register_base(atlas_core.sqlalchemy.BaseModel)

        # Create empty databases if not created
        if app.debug:
            atlas_core.create_db(app, atlas_core.db)

    return app

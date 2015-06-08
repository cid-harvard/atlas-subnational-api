import atlas_core
from atlas_core.helpers.flask import handle_api_error, APIError
from .metadata.views import metadata_app
from .data.views import products_app, departments_app

from .core import db, cache


def create_app(config={}):

    # Create base app from atlas_core
    app = atlas_core.create_app(additional_config=config, name="colombia",
                                standalone=True)

    cache.init_app(app)

    # API Endpoints
    app.register_blueprint(metadata_app, url_prefix="/metadata")
    app.register_blueprint(products_app, url_prefix="/data")
    app.register_blueprint(departments_app, url_prefix="/data")


    # CORS hook for debug reasons.
    @app.after_request
    def cors(response):
        if app.debug:
            response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    # Register error handler to return proper json-api errors on abort()
    app.errorhandler(APIError)(handle_api_error)

    with app.app_context():
        # Register sqlalchemy model base so that models in this project that
        # use it are also registered
        db.register_base(atlas_core.sqlalchemy.BaseModel)

        # Create empty databases if not created
        if app.debug:
            atlas_core.create_db(app, db)

    return app

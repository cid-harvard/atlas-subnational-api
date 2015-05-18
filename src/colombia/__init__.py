import atlas_core
from colombia.views import (products_app, metadata_app)

from .core import db, cache

def create_app(config={}):

    # Create base app from atlas_core
    app = atlas_core.create_app(additional_config=config, name="colombia")

    cache.init_app(app)

    # API Endpoints
    app.register_blueprint(metadata_app)
    app.register_blueprint(products_app)

    # CORS hook for debug reasons.
    @app.after_request
    def cors(response):
        if app.debug:
            response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    with app.app_context():
        # Register sqlalchemy model base so that models in this project that
        # use it are also registered
        db.register_base(atlas_core.sqlalchemy.BaseModel)

        # Create empty databases if not created
        if app.debug:
            atlas_core.create_db(app, db)

    return app

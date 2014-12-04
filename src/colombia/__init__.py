from flask import Flask

from flask_debugtoolbar import DebugToolbarExtension
from werkzeug.contrib.profiler import ProfilerMiddleware


def create_app(config={}):
    app = Flask("colombia")
    app.config.from_envvar("FLASK_CONFIG")
    app.config.update(config)

    from colombia.ext import api, cache, db, restless_api

    cache.init_app(app)
    db.init_app(app)

    # API Endpoints
    from colombia.views import CatAPI
    api.add_resource(CatAPI, "/cats/<int:cat_id>")
    # Workaround for weird bug, instead of init_app
    # https://github.com/flask-restful/flask-restful/issues/357
    api.blueprint = app

    #from colombia.models import HSProduct
    #restless_api.init_app(app, flask_sqlalchemy_db=db)
    #restless_api.create_api(HSProduct, ['GET'], exclude_columns=["en", "es"],
    #                        results_per_page=-1)

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

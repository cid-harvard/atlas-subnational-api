DEBUG = True
SECRET_KEY = 'lalalalalalala'

SENTRY_DSN = ""

SQLALCHEMY_ECHO = True
SQLALCHEMY_DATABASE_URI = "postgresql://postgres:postgres@localhost/colombia"

SQLALCHEMY_BINDS = {
    'text_search':        'postgresql://postgres:postgres@localhost/sqlalchemy_searchable_text'
}
#SQLALCHEMY_DATABASE_URI = "sqlite:///database.db"
#"sqlite:///database.db"
#'postgresql://postgres:postgres@localhost/sqlalchemy_searchable_text'
SQLALCHEMY_TRACK_MODIFICATIONS = False

CACHE_TYPE = "simple"
CACHE_KEY_PREFIX = "colombia1::"

DEBUG_TB_ENABLED = DEBUG
PROFILE = False
PORT = 8001

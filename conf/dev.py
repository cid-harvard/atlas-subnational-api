DEBUG = True
SECRET_KEY = 'lalalalalalala'

SENTRY_DSN = ""

SQLALCHEMY_ECHO = False
SQLALCHEMY_DATABASE_URI = "sqlite:///database.db"
SQLALCHEMY_TRACK_MODIFICATIONS = False

CACHE_TYPE = "simple"
CACHE_KEY_PREFIX = "colombia::"

DEBUG_TB_ENABLED = DEBUG
PROFILE = False
PORT = 8001

# Ingestion settings
DATASET_ROOT = "/nfs/home/M/makmanalp/shared_space/cid_colombia/Mali/2016Output"
YEAR_MIN_TRADE = 2007
YEAR_MAX_TRADE = 2016
YEAR_MIN_INDUSTRY = 2007
YEAR_MAX_INDUSTRY = 2016
YEAR_MIN_DEMOGRAPHIC = 2007
YEAR_MAX_DEMOGRAPHIC = 2016
YEAR_MIN_AGPRODUCT = 2007
YEAR_MAX_AGPRODUCT = 2015

# Agricultural census is already only a single year but we need this to know
# which year we're using
YEAR_AGRICULTURAL_CENSUS = 2014

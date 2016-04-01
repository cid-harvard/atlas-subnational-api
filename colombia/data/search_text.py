from atlas_core.sqlalchemy import BaseModel
from atlas_core.model_mixins import IDMixin, LanguageMixin

from sqlalchemy.ext.hybrid import hybrid_method
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_searchable import make_searchable
from sqlalchemy_utils.types import TSVectorType

from sqlalchemy import cast
import psycopg2,json

# sudo apt-get install python3-dev
# to get psycopg2 in case the installation fails


db = SQLAlchemy()

Base = declarative_base()

make_searchable()

class Article(Base):
    """docstring for ""ArticleiBaseself, arg):
        super(, self).Article)Basef.arg = arg
    """
    __tablename__ = 'article_1'

    id = sa.Column(sa.Integer, primary_key= True)
    name = sa.Column(sa.Unicode(255))
    content = sa.Column(sa.UnicodeText)
    search_vector = sa.Column(TSVectorType('name', 'content'))


class I18nMixinBase(object):


    #name_en = db.Column(db.UnicodeText)
    #name_short_en = db.Column(db.Unicode(50))
    #description_en = db.Column(db.UnicodeText)

    @hybrid_method
    def get_localized(self, field, lang):
        """Look up the language localized version of a field by looking up
        field_lang."""
        return getattr(self, field + "_" + lang)

    @staticmethod
    def create(fields, languages=["en"], class_name="I18nMixin"):
        localized_fields = {}
        for name, value in fields.items():
            for language in languages:
                field_name = name + "_" + language
                localized_fields[field_name] = sa.Column(value)
        #localized_fields['search_vector'] = sa.Column(TSVectorType('name_short_en'))
        return type(class_name, (I18nMixinBase,), localized_fields)


I18nMixin = I18nMixinBase.create(
    languages=["en", "es", "de"],
    fields={
        "name": sa.UnicodeText,
        "name_short": sa.UnicodeText,
        "description": sa.UnicodeText
    })


class Metadata(Base, IDMixin, I18nMixin):
    """Baseclass for all entity metadata models. Any subclass of this class
    must have two fields:
        - a LEVELS = [] list that contains all the classification levels as
        strings
        - a db.Column(db.Enum(*LEVELS)) enum field
    """

    __abstract__ = True

    code = sa.Column(sa.Unicode(25))
    parent_id = sa.Column(sa.Integer)


class HSProduct1(Metadata):
    """A product according to the HS4 (Harmonized System) classification.
    Details can be found here: http://www.wcoomd.org/en/topics/nomenclature/instrument-and-tools/hs_nomenclature_2012/hs_nomenclature_table_2012.aspx
    """
    __bind_key__ = 'text_search'
    __tablename__ = "product_test"

    #: Possible aggregation levels
    LEVELS = [
        "section",
        "2digit",
        "4digit"
    ]
    level = sa.Column(sa.Enum(*LEVELS,name="product_level"))
    # This is the column where both name_short and name_en are stored
    name_en_test = sa.Column(sa.UnicodeText)
    name_es_test = sa.Column(sa.UnicodeText)
    #my_enum = sa.Enum('country','municipality', 'department', 'population_center', name='my_enum')

    en_search_vector = sa.Column(TSVectorType('name_en_test',regconfig='pg_catalog.english'))
    es_search_vector = sa.Column(TSVectorType('name_es_test', regconfig='pg_catalog.spanish'))

from sqlalchemy.dialects.postgresql import ENUM

#from flask.ext.sqlalchemy import SQLAlchemy, BaseQuery
from sqlalchemy_searchable import search
#from sqlalchemy_searchable import SearchQueryMixin

#class LocationQuery(BaseQuery, SearchQueryMixin):
#    pass

class Location1(Metadata):
    """A geographical location. Locations have multiple levels:

    A municipality is the smallest unit of location we have and has a 5-digit
    code. Cities often contain multiple municipalities, but there are also
    standalone municipalities that are not part of any city.

    A department is a grouping of municipalities to create 32ish areas of the
    country. Departments in Colombia have 2 digit codes, which are the first 2
    digits of the 5-digit codes of the constituent municipalities."""

     # query_class = LocationQuery
    __bind_key__ = 'text_search'
    __tablename__ = "location_test"


    #: Possible aggregation levels
    LEVELS = [
        "country",
        "municipality",
        "department",
        "population_center"
    ]
    level = sa.Column(sa.Enum(*LEVELS, name="location_level"))
    name_en_test = sa.Column(sa.UnicodeText)
    name_es_test = sa.Column(sa.UnicodeText)
    #my_enum = sa.Enum('country','municipality', 'department', 'population_center', name='my_enum')
    en_search_vector = sa.Column(TSVectorType('name_en_test',regconfig='pg_catalog.english'))
    es_search_vector = sa.Column(TSVectorType('name_es_test', regconfig='pg_catalog.spanish'))

    #search_vector = sa.Column(TSVectorType('name_short_en_test'))

class Industry1(Metadata):
    """An ISIC 4 industry."""
    __bind_key__ = 'text_search'
    __tablename__ = "industry_test"


    #: Possible aggregation levels
    LEVELS = [
        "section",
        "division",
        "group",
        "class"
    ]
    #level = db.Column(db.Enum(*LEVELS))
    level = db.Column(db.Enum(*LEVELS, name="industry_level"))
    name_en_test = db.Column(db.UnicodeText)
    name_es_test = db.Column(db.UnicodeText)
    #search_vector = sa.Column(TSVectorType('name_en_test'))
    en_search_vector = sa.Column(TSVectorType('name_en_test',regconfig='pg_catalog.english'))
    es_search_vector = sa.Column(TSVectorType('name_es_test', regconfig='pg_catalog.spanish'))

#SQLALCHEMY_DATABASE_URI = "postgresql://postgres:postgres@localhost/colombia"

#SQLALCHEMY_BINDS = {
#    'text_search':        'postgresql://postgres:postgres@localhost/sqlalchemy_searchable_text'
#}
engine2 = create_engine('postgresql://postgres:postgres@localhost/atlas')
#engine2 = create_engine('postgresql://postgres:postgres@localhost/test4')
#engine = create_engine(bind=['text_search'])
#app = Flask(__name__)
#db = SQLAlchemy(app)
sa.orm.configure_mappers()  # Very important
Base.metadata.create_all(engine2) # this is where things get created.
#db.create_all(bind=['text_search'])


def do_posgres_update():
    pass
    Session = sessionmaker(bind = engine2)
    session = Session()

    article1 = Article(name=u'third article', content=u'this is the third article')
    article2 = Article(name=u'fourth article', content=u'This is the fourth article')

    #session.add(article1)
    #session.add(article2)
    #session.commit()

# This needs to be done for each of the language
#search_vector = TSVectorType('name', regconfig='pg_catalog.finnish')

def do_location_query(search_str, lang) :
    Session = sessionmaker(bind = engine2)
    session = Session()

    query_location = session.query(Location1)
    if lang == 'en-col':
        print('location en-col')
        query_location = search(query_location, search_str,sort=True)
    else :
        print('location es-col')
        query_location = search(query_location,search_str, vector=Location1.es_search_vector,sort=True)

    #query_location = search(query_location, search_str,sort=True)
    #print (query_location.first().name_short_en_test)
    rl = query_location.all()
    #print (rl)
    #for r in rl :
    #    print (r.name_short_en_test)

    return [{ "type": "location",
              "name_en": x.description_en,
              "name_es": x.description_es,
              "code": x.code,
              "description_en": x.name_short_en,
              "description_es": x.description_es,
              "level":x.level,
              "id": x.id,
              "name_short_en": x.description_en,
              "name_short_es": x.description_es,
              "parent_id": x.parent_id}
            for x in rl]
    #print (Location.query.search(u'pri').limit(5).all())

def do_product_query(search_str,lang) :
    Session = sessionmaker(bind = engine2)
    session = Session()

    query_product = session.query(HSProduct1)
    if lang == 'en-col':
        query_product = search(query_product, search_str,sort=True)
    else :
        query_product = search(query_product,search_str, vector=HSProduct1.es_search_vector,sort=True)

    #print (query_product.first().name_en_test)
    rl = query_product.all()
    #print (rl)
    for r in rl :
        print (r.name_en_test)
    from flask import jsonify
    #return dict(product=[x.name_en_test for x in rl])
    return [{
    "type": "product",
    "name_en": x.description_en,
    "name_es": x.description_es,
                   "code": x.code,
                   "description_en": x.name_short_en,
                   "description_es": x.description_es,
                   "level":x.level,
                   "id": x.id,

                   "name_short_en": x.description_en,
                   "name_short_es": x.description_es,
                               "parent_id": x.parent_id} for x in rl]

def do_industry_query(search_str,lang) :
    Session = sessionmaker(bind = engine2)
    session = Session()

    query_industry = session.query(Industry1)
    #query_industry = search(query_industry, search_str,sort=True)
    if lang == 'en-col':
        query_industry = search(query_industry, search_str,sort=True)
    else :
        query_industry = search(query_industry,search_str, vector=Industry1.es_search_vector,sort=True)
    #print (query_industry.first().name_en_test)
    rl = query_industry.all()
    return [{#"name":x.description_en,
             "type": "industry",
             "name_en": x.description_en,
             "name_es": x.description_es,
                            "code": x.code,
                            "description_en": x.name_short_en,
                            "description_es": x.description_es,
                            "level":x.level,
                            "id": x.id,

                            "name_short_en": x.description_en,
                            "name_short_es": x.description_es,
                            "parent_id": x.parent_id} for x in rl]

from sqlalchemy_searchable import parse_search_query

def combined_search_query(search_str,lang,filter):
    Session = sessionmaker(bind = engine2)
    session = Session()
    #results_location = do_location_query(search_str)
    #results_industry = do_industry_query(search_str)

    if filter == 'industry':
        results_industry = do_industry_query(search_str,lang)
        #return json.dumps(dict({"industry":results_industry}))
        return json.dumps(dict({"textsearch":results_industry}))
    elif filter == 'product' :
        results_product = do_product_query(search_str,lang)
        #results = dict({"textsearch":results_product})
        #return json.dumps(dict({"textsearch":{"product":results_product}}))
        return json.dumps(dict({"textsearch":results_product}))
    elif filter == 'location' :
        result_location = do_location_query(search_str,lang)
        #return json.dumps(dict({"location":result_location}))
        return json.dumps(dict({"textsearch":result_location}))
    else :
        results_product = do_product_query(search_str,lang)
        results_industry = do_industry_query(search_str,lang)
        result_location = do_location_query(search_str,lang)
        results_product.extend(results_industry)
        results_product.extend(result_location)
        results = dict({"textsearch":results_product})
        return json.dumps(results)


    #results = [results_industry,results_product,results_location]

    #from flask import jsonify
    #return jsonify(textsearch=results)

    # Try giving location on top
    #combined_search_vector = ( Industry.search_vector |  sa.func.coalesce(Product.search_vector,u'')
    #results = (
    #    session.query(Industry)
    #    .join(Product)
    #    .filter(
    #        combined_search_vector(
    #            parse_search_query('education')
    #        )
    #    )
    #)

    #print (industry_query)

#combined_search_query('fabric')

#do_posgres_update()

#do_location_query()
#do_product_query()
#do_industry_query()

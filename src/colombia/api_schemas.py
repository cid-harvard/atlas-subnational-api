from flask.ext.restful import fields

hs_product_fields = {
    'code': fields.String,
    'section_code': fields.String,
    'section_name': fields.String,
    'section_name_es': fields.String,
    'id': fields.Integer,
    'name': fields.String,
    'en': fields.String,
    'es': fields.String,
    'aggregation': fields.String,
}

department_fields = {
    'code': fields.String,
    'id': fields.Integer,
    'name': fields.String,
    'population': fields.Integer,
    'gdp': fields.Integer,
}

department_product_year_fields = {
    'import_value': fields.Integer,
    'export_value': fields.Integer,
    'export_rca': fields.Float,
    'distance': fields.Float,
    'cog': fields.Float,
    'coi': fields.Float,

    'id': fields.Integer,
    'department_id': fields.String,
    'product_id': fields.String,
    'year': fields.Integer
}


product_year_fields = {
    'pci': fields.Float,

    'id': fields.Integer,
    'product_id': fields.String,
    'year': fields.Integer
}

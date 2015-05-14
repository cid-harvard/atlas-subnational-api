
import marshmallow as ma
from marshmallow import fields


class DepartmentProductYearSchema(ma.Schema):

    class Meta:
        fields = ("import_value", "export_value", "export_rca", "distance",
                  "cog", "coi", "department_id", "product_id", "year")


class HSProductFields(ma.Schema):

    class Meta:
        fields = ("code", "section_code", "section_name", "section_name_es",
                  "id", "name", "en", "es", "aggregation")


department_product_year = DepartmentProductYearSchema(many=True)
hs_product = HSProductFields(many=True)

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

from flask import jsonify
import marshmallow as ma

from atlas_core.helpers.flask import APIError


def marshal(schema, data, json=True, many=True):
    """Shortcut to marshals a marshmallow schema and dump out a flask json
    response, or raise an APIError with appropriate messages otherwise."""

    try:
        serialization_result = schema.dump(data, many=many)
    except ma.ValidationError as err:
        raise APIError(message=err.messages)

    if json:
        return jsonify(data=serialization_result.data)
    else:
        return serialization_result.data


class DepartmentProductYearSchema(ma.Schema):

    class Meta:
        fields = ("import_value", "export_value", "export_rca", "distance",
                  "cog", "coi", "department_id", "product_id", "year")


class HSProductSchema(ma.Schema):

    class Meta:
        fields = ("code", "section_code", "section_name", "section_name_es",
                  "id", "name", "en", "es", "aggregation")


class DepartmentSchema(ma.Schema):

    class Meta:
        fields = ("code", "id", "name", "population", "gdp")


class ProductYearSchema(ma.Schema):

    class Meta:
        fields = ("pci", "id", "product_id", "year")


department_product_year = DepartmentProductYearSchema(many=True)
product_year = ProductYearSchema(many=True)
hs_product = HSProductSchema(many=True)
department = DepartmentSchema(many=True)

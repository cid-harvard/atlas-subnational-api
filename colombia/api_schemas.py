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


class MunicipalityProductYearSchema(ma.Schema):

    class Meta:
        fields = ("import_value", "export_value", "export_rca", "distance",
                  "cog", "coi", "municipality_id", "product_id", "year")


class DepartmentIndustryYearSchema(ma.Schema):

    class Meta:
        fields = ("employment", "wages", "rca", "distance", "cog", "coi",
                  "department_id", "industry_id", "year")


class MunicipalityIndustryYearSchema(ma.Schema):

    class Meta:
        fields = ("employment", "wages", "rca", "distance", "cog", "coi",
                  "municipality_id", "industry_id", "year")


class DepartmentSchema(ma.Schema):

    class Meta:
        fields = ("code", "id", "name", "population", "gdp")


class ProductYearSchema(ma.Schema):

    class Meta:
        fields = ("pci", "product_id", "year")


class IndustryYearSchema(ma.Schema):

    class Meta:
        fields = ("complexity", "industry_id", "year")


class DepartmentYearSchema(ma.Schema):

    class Meta:
        fields = ("department_id", "year", "eci", "diversity", "gdp_nominal",
                  "gdp_real", "gdp_pc_nominal", "gdp_pc_real", "population")


class MetadataSchema(ma.Schema):
    """Base serialization schema for metadata APIs."""

    class Meta:
        additional = ("id", "code", "level", "parent_id")


class ColombiaMetadataSchema(MetadataSchema):

    name_en = ma.fields.Str(required=False)
    name_short_en = ma.fields.Str(required=False)
    description_en = ma.fields.Str(required=False)

    name_es = ma.fields.Str(required=False)
    name_short_es = ma.fields.Str(required=False)
    description_es = ma.fields.Str(required=False)


department_product_year = DepartmentProductYearSchema(many=True)
municipality_product_year = MunicipalityProductYearSchema(many=True)
department_industry_year = DepartmentIndustryYearSchema(many=True)
municipality_industry_year = MunicipalityIndustryYearSchema(many=True)
product_year = ProductYearSchema(many=True)
industry_year = IndustryYearSchema(many=True)
department_year = DepartmentYearSchema(many=True)
department = DepartmentSchema(many=True)
metadata = ColombiaMetadataSchema(many=True)

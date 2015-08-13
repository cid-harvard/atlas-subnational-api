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


XPY_FIELDS = ("import_value", "export_value", "import_num_plants",
              "export_num_plants", "export_rca", "distance", "cog", "coi",
              "product_id", "year")


class CountryProductYearSchema(ma.Schema):

    country_id = ma.fields.Constant(0)
    export_rca = ma.fields.Constant(None)
    distance = ma.fields.Constant(None)
    cog = ma.fields.Constant(None)
    coi = ma.fields.Constant(None)

    class Meta:
        fields = XPY_FIELDS + ("country_id", )


class MSAProductYearSchema(ma.Schema):

    class Meta:
        fields = XPY_FIELDS + ("msa_id", )


class DepartmentProductYearSchema(ma.Schema):

    class Meta:
        fields = XPY_FIELDS + ("department_id", )


class MunicipalityProductYearSchema(ma.Schema):

    class Meta:
        fields = XPY_FIELDS + ("municipality_id", )


class CountryMunicipalityProductYearSchema(ma.Schema):

    class Meta:
        fields = ("export_value", "country_id", "municipality_id",
                  "product_id", "year")


class CountryDepartmentProductYearSchema(ma.Schema):

    class Meta:
        fields = ("export_value", "country_id", "department_id",
                  "product_id", "year")


XIY_FIELDS = ("employment", "wages", "monthly_wages", "num_establishments",
              "rca", "distance", "cog", "coi", "industry_id", "year")


class CountryIndustryYearSchema(ma.Schema):

    country_id = ma.fields.Constant(0)

    distance = ma.fields.Constant(None)
    cog = ma.fields.Constant(None)
    coi = ma.fields.Constant(None)
    rca = ma.fields.Constant(None)

    class Meta:
        fields = XIY_FIELDS + ("country_id", )


class DepartmentIndustryYearSchema(ma.Schema):

    class Meta:
        fields = XIY_FIELDS + ("department_id",)


class MSAIndustryYearSchema(ma.Schema):

    class Meta:
        fields = XIY_FIELDS + ("msa_id",)


class MunicipalityIndustryYearSchema(ma.Schema):

    class Meta:
        fields = XIY_FIELDS + ("municipality_id",)


class DepartmentSchema(ma.Schema):

    class Meta:
        fields = ("code", "id", "name", "population", "gdp")


class ProductYearSchema(ma.Schema):

    class Meta:
        fields = ("pci", "product_id", "year")


class IndustryYearSchema(ma.Schema):

    class Meta:
        fields = ("complexity", "employment", "wages", "monthly_wages",
                  "num_establishments", "industry_id", "year")


class DepartmentYearSchema(ma.Schema):

    class Meta:
        fields = ("department_id", "year", "eci", "diversity", "gdp_nominal",
                  "gdp_real", "gdp_pc_nominal", "gdp_pc_real", "population",
                  "employment", "wages", "monthly_wages", "num_establishments")


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


country_product_year = CountryProductYearSchema(many=True)
department_product_year = DepartmentProductYearSchema(many=True)
msa_product_year = MSAProductYearSchema(many=True)
municipality_product_year = MunicipalityProductYearSchema(many=True)

country_municipality_product_year = CountryMunicipalityProductYearSchema(many=True)
country_department_product_year = CountryDepartmentProductYearSchema(many=True)

country_industry_year = CountryIndustryYearSchema(many=True)
department_industry_year = DepartmentIndustryYearSchema(many=True)
msa_industry_year = MSAIndustryYearSchema(many=True)
municipality_industry_year = MunicipalityIndustryYearSchema(many=True)

product_year = ProductYearSchema(many=True)
industry_year = IndustryYearSchema(many=True)
department_year = DepartmentYearSchema(many=True)

department = DepartmentSchema(many=True)
metadata = ColombiaMetadataSchema(many=True)

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


def fix_id_hook(self, data):
    """This is here to handle data model to legacy API compatibility. New
    data always has location_id for consistency, vs the API uses msa_id,
    department_id etc. This converts the former to the latter. To get this
    to work, you need to set schema.context["id_field_name"]."""

    key = self.context.get("id_field_name", None)
    if key is None:
        raise ma.ValidationError(
            "Please set schema.context['id_field_name']."
        )

    value = data["location_id"]
    del data["location_id"]

    data[key] = value
    return data


class XProductYearSchema(ma.Schema):

    location_id = ma.fields.Integer(default=0)
    export_rca = ma.fields.Float(default=None)
    distance = ma.fields.Float(default=None)
    cog = ma.fields.Float(default=None)

    fix_id_hook = ma.post_dump(fix_id_hook)

    class Meta:
        fields = ("import_value", "export_value", "import_num_plants",
                  "export_num_plants", "export_rca", "distance", "cog",
                  "product_id", "location_id", "year")


class XLivestockYearSchema(ma.Schema):

    class Meta:
        fields = ("num_livestock", "num_farms", "livestock_id", "location_id")


class CountryMunicipalityProductYearSchema(ma.Schema):

    municipality_id = ma.fields.Integer(attribute="location_id")

    export_value = ma.fields.Float(default=None)
    import_value = ma.fields.Float(default=None)

    class Meta:
        fields = ("export_value", "import_value", "country_id", "product_id", "year")


class CountryDepartmentProductYearSchema(ma.Schema):

    department_id = ma.fields.Integer(attribute="location_id")

    export_value = ma.fields.Float(default=None)
    import_value = ma.fields.Float(default=None)

    class Meta:
        fields = ("export_value", "import_value", "country_id", "product_id", "year")


class CountryMSAProductYearSchema(ma.Schema):

    msa_id = ma.fields.Integer(attribute="location_id")

    export_value = ma.fields.Float(default=None)
    import_value = ma.fields.Float(default=None)

    class Meta:
        fields = ("export_value", "import_value", "country_id", "product_id", "year")


class PartnerProductYearSchema(ma.Schema):

    department_id = ma.fields.Integer(attribute="location_id")

    class Meta:
        fields = ("export_value", "import_value", "export_num_plants",
                  "import_num_plants", "country_id", "product_id", "year")


class CountryXYearSchema(ma.Schema):

    class Meta:
        fields = ("export_value", "import_value", "country_id", "location_id", "year")


class XIndustryYearSchema(ma.Schema):

    location_id = ma.fields.Integer(default=0)

    distance = ma.fields.Float(default=None)
    cog = ma.fields.Float(default=None)
    rca = ma.fields.Float(default=None)

    fix_id_hook = ma.post_dump(fix_id_hook)

    class Meta:
        fields = ("employment", "wages", "monthly_wages", "num_establishments",
                  "rca", "distance", "cog", "industry_id", "location_id",
                  "year")


class ProductYearSchema(ma.Schema):

    class Meta:
        fields = ("pci", "product_id", "year")


class IndustryYearSchema(ma.Schema):

    class Meta:
        fields = ("complexity", "employment", "wages", "monthly_wages",
                  "num_establishments", "industry_id", "year")


class DepartmentYearSchema(ma.Schema):

    department_id = ma.fields.Integer(attribute="location_id")

    class Meta:
        fields = ("year", "eci", "diversity", "gdp_nominal", "gdp_real",
                  "gdp_pc_nominal", "gdp_pc_real", "population", "employment",
                  "wages", "monthly_wages", "num_establishments",
                  "industry_eci", "coi", "industry_coi", "department_id")


class MSAYearSchema(ma.Schema):

    class Meta:
        fields = ("year", "eci", "industry_eci", "employment", "wages",
                  "monthly_wages", "num_establishments", "coi", "industry_coi",
                  "location_id")


class OccupationYearSchema(ma.Schema):

    class Meta:
        fields = ("occupation_id", "average_wages", "num_vacancies")


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


country_municipality_product_year = CountryMunicipalityProductYearSchema(many=True)
country_department_product_year = CountryDepartmentProductYearSchema(many=True)
country_msa_product_year = CountryMSAProductYearSchema(many=True)

country_x_year = CountryXYearSchema(many=True)

product_year = ProductYearSchema(many=True)
industry_year = IndustryYearSchema(many=True)
occupation_year = OccupationYearSchema(many=True)
department_year = DepartmentYearSchema(many=True)
msa_year = MSAYearSchema(many=True)

metadata = ColombiaMetadataSchema(many=True)

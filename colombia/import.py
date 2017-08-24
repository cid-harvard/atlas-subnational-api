from colombia import models, create_app
from colombia.core import db

from dataset_tools import (process_dataset, classification_to_models)

from datasets import (trade4digit_country, trade4digit_department,
                      trade4digit_msa, trade4digit_municipality,
                      industry4digit_country, industry4digit_department,
                      industry4digit_msa, industry2digit_department,
                      industry4digit_municipality,
                      trade4digit_rcpy_municipality,
                      industry2digit_msa,
                      trade4digit_rcpy_department, trade4digit_rcpy_msa,
                      trade4digit_rcpy_country, population,
                      gdp_nominal_department, gdp_real_department,
                      occupation2digit, occupation2digit_industry2digit,
                      industry2digit_country)

from datasets import (product_classification,
                      industry_classification,
                      location_classification,
                      country_classification,
                      occupation_classification
                      )

if __name__ == "__main__":

        app = create_app()
        with app.app_context():

            products = classification_to_models(product_classification,
                                                models.HSProduct)
            db.session.add_all(products)
            db.session.commit()

            locations = classification_to_models(location_classification,
                                                 models.Location)
            db.session.add_all(locations)
            db.session.commit()

            industries = classification_to_models(industry_classification,
                                                  models.Industry)
            db.session.add_all(industries)
            db.session.commit()

            occupations = classification_to_models(occupation_classification,
                                                  models.Occupation)
            db.session.add_all(occupations)
            db.session.commit()


            countries = classification_to_models(country_classification,
                                                  models.Country)
            db.session.add_all(countries)
            db.session.commit()

            # Country product year
            ret = process_dataset(trade4digit_country)

            df = ret[('location_id', 'product_id', 'year')].reset_index()
            df["level"] = "4digit"
            df.to_sql("country_product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Department product year
            ret = process_dataset(trade4digit_department)

            df = ret[('product_id', 'year')].reset_index()
            df["level"] = "4digit"
            df.to_sql("product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            df = ret[('location_id', 'product_id', 'year')].reset_index()
            df["level"] = "4digit"
            df.to_sql("department_product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Department-year product
            dy_p = ret[('location_id', 'year')].reset_index()

            # Department - year industry
            ret = process_dataset(industry4digit_department)
            dy_i = ret[('location_id', 'year')].reset_index()

            # GDP data
            ret = process_dataset(gdp_real_department)
            gdp_real_df = ret[('location_id', 'year')]

            ret = process_dataset(gdp_nominal_department)
            gdp_nominal_df = ret[('location_id', 'year')]

            gdp_df = gdp_real_df.join(gdp_nominal_df).reset_index()

            # Pop data
            ret = process_dataset(population)
            pop_df = ret[('location_id', 'year')].reset_index()

            # Merge all dept-year variables together
            def filter_year_range(df, min_year, max_year):
                return dy_p[(min_year <= df.year) & (df.year <= max_year)]

            c = app.config
            df_p = filter_year_range(dy_p, c["YEAR_MIN_TRADE"], c["YEAR_MAX_TRADE"])
            df_i = filter_year_range(dy_i, c["YEAR_MIN_INDUSTRY"], c["YEAR_MAX_INDUSTRY"])
            gdp_df = filter_year_range(gdp_df, c["YEAR_MIN_DEMOGRAPHIC"], c["YEAR_MAX_DEMOGRAPHIC"])
            pop_df = filter_year_range(pop_df, c["YEAR_MIN_DEMOGRAPHIC"], c["YEAR_MAX_DEMOGRAPHIC"])

            dy = dy_p.merge(dy_i, on=["location_id", "year"], how="outer")
            dy = dy.merge(gdp_df, on=["location_id", "year"], how="outer")
            dy = dy.merge(pop_df, on=["location_id", "year"], how="outer")

            dy["gdp_pc_nominal"] = dy.gdp_nominal / dy.population
            dy["gdp_pc_real"] = dy.gdp_real / dy.population

            dy.to_sql("department_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Municipality product year
            ret = process_dataset(trade4digit_municipality)

            df = ret[('location_id', 'product_id', 'year')].reset_index()
            df["level"] = "4digit"
            df.to_sql("municipality_product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # MSA product year
            ret = process_dataset(trade4digit_msa)

            df = ret[('location_id', 'product_id', 'year')].reset_index()
            df["level"] = "4digit"
            df.to_sql("msa_product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            trade_msa_year = ret[('location_id', 'year')]

            # Country - trade rcpy
            ret = process_dataset(trade4digit_rcpy_country)

            df = ret[("country_id", "location_id", "year")].reset_index()
            df.to_sql("country_country_year", db.engine,
                      index=False, chunksize=10000, if_exists="append")

            df = ret[("product_id", "country_id", "year")].reset_index()
            df["level"] = "4digit"
            df.to_sql("partner_product_year", db.engine,
                      index=False, chunksize=10000, if_exists="append")

            # MSA - trade rcpy
            ret = process_dataset(trade4digit_rcpy_msa)

            df = ret[("country_id", "location_id", "year")].reset_index()
            df.to_sql("country_msa_year", db.engine,
                      index=False, chunksize=10000, if_exists="append")

            # Municipality - trade rcpy
            ret = process_dataset(trade4digit_rcpy_municipality)

            df = ret[("country_id", "location_id", "year")].reset_index()
            df.to_sql("country_municipality_year", db.engine,
                      index=False, chunksize=10000, if_exists="append")

            df = ret[("country_id", "location_id", "product_id", "year")].reset_index()
            df["level"] = "4digit"
            df.to_sql("country_municipality_product_year", db.engine,
                      index=False, chunksize=10000, if_exists="append")

            # Department - trade rcpy
            ret = process_dataset(trade4digit_rcpy_department)

            df = ret[("country_id", "location_id", "product_id", "year")].reset_index()
            df["level"] = "4digit"
            df.to_sql("country_department_product_year", db.engine,
                      index=False, chunksize=10000, if_exists="append")

            df = ret[("country_id", "location_id", "year")].reset_index()
            df.to_sql("country_department_year", db.engine,
                      index=False, chunksize=10000, if_exists="append")

            # Country - industry- y ear
            ret = process_dataset(industry4digit_country)
            df = ret[('location_id', 'industry_id', 'year')].reset_index()
            df["level"] = "class"
            df.to_sql("country_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Department - industry - year
            ret = process_dataset(industry4digit_department)

            df = ret[('industry_id', 'year')].reset_index()
            df["level"] = "class"
            df.to_sql("industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            df = ret[('location_id', 'industry_id', 'year')].reset_index()
            df["level"] = "class"
            df.to_sql("department_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Country - two digit industry - year
            ret = process_dataset(industry2digit_country)
            df = ret[('industry_id', 'year')].reset_index()
            df["level"] = "division"
            df.to_sql("industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Department - two digit industry - year
            ret = process_dataset(industry2digit_department)

            df = ret[('location_id', 'industry_id', 'year')].reset_index()
            df["level"] = "division"
            df.to_sql("department_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # MSA - two digit industry - year
            ret = process_dataset(industry2digit_msa)

            df = ret[('location_id', 'industry_id', 'year')].reset_index()
            df["level"] = "division"
            df.to_sql("msa_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # MSA - industry - year
            ret = process_dataset(industry4digit_msa)

            df = ret[('location_id', 'industry_id', 'year')].reset_index()
            df["level"] = "class"
            df.to_sql("msa_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            industry_msa_year = ret[('location_id', 'year')]

            # MSA year
            msa_year = industry_msa_year.join(trade_msa_year).reset_index()
            msa_year.to_sql("msa_year", db.engine, index=False,
                            chunksize=10000, if_exists="append")


            # Municipality - industry - year
            ret = process_dataset(industry4digit_municipality)
            df = ret[('location_id', 'industry_id', 'year')].reset_index()
            df["level"] = "class"
            df.to_sql("municipality_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")


            # Occupation - year
            ret = process_dataset(occupation2digit)
            df = ret[('occupation_id')].reset_index()
            df["level"] = "minor_group"
            df.to_sql("occupation_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Occupation - industry - year
            ret = process_dataset(occupation2digit_industry2digit)
            df = ret[('occupation_id', 'industry_id')].reset_index()
            df["level"] = "minor_group"
            df.to_sql("occupation_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")


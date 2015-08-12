from colombia import models, create_app
from colombia.core import db

from dataset_tools import process_dataset, classification_to_models
from datasets import (trade4digit_department, trade4digit_msa,
                      trade4digit_municipality, industry4digit_department,
                      industry4digit_msa, industry2digit_department,
                      industry4digit_municipality,
                      trade4digit_rcpy_municipality,
                      trade4digit_rcpy_department, population, gdp_department)

from datasets import (product_classification,
                      industry_classification,
                      location_classification,
                      country_classification)

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

            countries = classification_to_models(country_classification,
                                                  models.Country)
            db.session.add_all(countries)
            db.session.commit()


            # Department product year
            ret = process_dataset(trade4digit_department)

            df = ret[('product_id', 'year')].reset_index()
            df["level"] = "4digit"
            df.to_sql("product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            df = ret[('department_id', 'product_id', 'year')].reset_index()
            df["level"] = "4digit"
            df.to_sql("department_product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Department-year
            dy = ret[('department_id', 'year')].reset_index()

            # GDP data
            ret = process_dataset(gdp_department)
            gdp_df = ret[('department_id', 'year')].reset_index()

            # Pop data
            ret = process_dataset(population)
            pop_df = ret[('department_id', 'year')].reset_index()

            # Merge all dept-year variables together
            dy = dy[(2007 <= dy.year) & (dy.year <= 2013)]
            gdp_df = gdp_df[(2007 <= gdp_df.year) & (gdp_df.year <= 2013)]
            pop_df = pop_df[(2007 <= pop_df.year) & (pop_df.year <= 2013)]
            dy = dy.merge(pop_df, on=["department_id", "year"], how="outer")
            dy = dy.merge(gdp_df, on=["department_id", "year"], how="outer")

            dy["gdp_pc_nominal"] = dy.gdp_nominal / dy.population

            dy.to_sql("department_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Municipality product year
            ret = process_dataset(trade4digit_municipality)

            df = ret[('municipality_id', 'product_id', 'year')].reset_index()
            df["level"] = "4digit"
            df.to_sql("municipality_product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # MSA product year
            ret = process_dataset(trade4digit_msa)

            df = ret[('msa_id', 'product_id', 'year')].reset_index()
            df["level"] = "4digit"
            df.to_sql("msa_product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")


            # Municipality - trade rcpy
            ret = process_dataset(trade4digit_rcpy_municipality)

            df = ret[("country_id", "municipality_id", "product_id", "year")].reset_index()
            df["level"] = "4digit"
            df.to_sql("country_municipality_product_year", db.engine,
                      index=False, chunksize=10000, if_exists="append")

            # Department - trade rcpy
            ret = process_dataset(trade4digit_rcpy_department)

            df = ret[("country_id", "department_id", "product_id", "year")].reset_index()
            df["level"] = "4digit"
            df.to_sql("country_department_product_year", db.engine,
                      index=False, chunksize=10000, if_exists="append")

            # Department - industry - year
            ret = process_dataset(industry4digit_department)

            df = ret[('industry_id', 'year')].reset_index()
            df["level"] = "class"
            df.to_sql("industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            df = ret[('department_id', 'industry_id', 'year')].reset_index()
            df["level"] = "class"
            df.to_sql("department_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Department - two digit industry - year
            ret = process_dataset(industry2digit_department)

            df = ret[('industry_id', 'year')].reset_index()
            df["level"] = "division"
            df.to_sql("industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            df = ret[('department_id', 'industry_id', 'year')].reset_index()
            df["level"] = "division"
            df.to_sql("department_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # MSA - industry - year
            ret = process_dataset(industry4digit_msa)

            df = ret[('msa_id', 'industry_id', 'year')].reset_index()
            df["level"] = "class"
            df.to_sql("msa_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")


            # Municipality - industry - year
            ret = process_dataset(industry4digit_municipality)
            df = ret[('municipality_id', 'industry_id', 'year')].reset_index()
            df["level"] = "class"
            df.to_sql("municipality_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")



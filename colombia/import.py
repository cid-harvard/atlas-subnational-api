from colombia import models, create_app
from colombia.core import db

from dataset_tools import process_dataset, classification_to_models
from datasets import (trade4digit_department, industry4digit_department,
                      industry4digit_municipality, population, gdp)

if __name__ == "__main__":

        app = create_app()
        with app.app_context():

            # Load classifications
            from datasets import (product_classification,
                                  industry_classification,
                                  location_classification)

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

            # Department product year
            ret = process_dataset(trade4digit_department)

            df = ret[('product_id', 'year')].reset_index()
            df.to_sql("product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            df = ret[('department_id', 'product_id', 'year')].reset_index()
            df.to_sql("department_product_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Department-year
            dy = ret[('department_id', 'year')].reset_index()

            # GDP data
            ret = process_dataset(gdp)
            gdp_df = ret[('department_id', 'year')].reset_index()

            gdp_df.gdp_real = gdp_df.gdp_real * (10 ** 6)
            gdp_df.gdp_nominal = gdp_df.gdp_nominal * (10 ** 6)

            # Pop data
            ret = process_dataset(population)
            pop_df = ret[('department_id', 'year')].reset_index()

            # Merge all dept-year variables together
            dy = dy[(2007 <= dy.year) & (dy.year <= 2013)]
            gdp_df = gdp_df[(2007 <= gdp_df.year) & (gdp_df.year <= 2013)]
            pop_df = pop_df[(2007 <= pop_df.year) & (pop_df.year <= 2013)]
            dy = dy.merge(pop_df, on=["department_id", "year"], how="outer")
            dy = dy.merge(gdp_df, on=["department_id", "year"], how="outer")

            dy["gdp_pc_real"] = dy.gdp_real / dy.population
            dy["gdp_pc_nominal"] = dy.gdp_nominal / dy.population

            dy.to_sql("department_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Department - industry - year
            ret = process_dataset(industry4digit_department)

            df = ret[('industry_id', 'year')].reset_index()
            df.to_sql("industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            df = ret[('department_id', 'industry_id', 'year')].reset_index()
            df.to_sql("department_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

            # Municipality - industry - year
            ret = process_dataset(industry4digit_municipality)
            df = ret[('municipality_id', 'industry_id', 'year')].reset_index()
            df.to_sql("municipality_industry_year", db.engine, index=False,
                      chunksize=10000, if_exists="append")

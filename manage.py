from colombia import create_app, models, core, factories
from flask.ext.script import Manager, Shell
from sqlalchemy.exc import SQLAlchemyError

import random

app = create_app()
manager = Manager(app)


def _make_context():
        from colombia import dataset_tools, datasets
        return dict(app=app, db=core.db, models=models, factories=factories, dataset_tools=dataset_tools, datasets=datasets)

manager.add_command("shell", Shell(make_context=_make_context))


@manager.option("-n", help="Number of dummy things")
def dummy(n=10):
    """Generate dummy data."""
    if not app.debug:
        raise Exception("Unsafe to generate dummy data while not in DEBUG.")

    # Generate a set of products and departments.
    departments = []
    for x in range(0, 6):
        departments.append(factories.Location(level="department"))

    sections = []
    for x in range(0, 4):
        sections.append(factories.HSProduct(level="section"))

    core.db.session.commit()

    two_digits = []
    for x in range(0, 8):
        parent_id = random.choice(sections).id
        two_digits.append(factories.HSProduct(level="2digit",
                                              parent_id=parent_id))

    core.db.session.commit()

    products = []
    for x in range(0, 20):
        parent_id = random.choice(two_digits).id
        products.append(factories.HSProduct(level="4digit",
                                            parent_id=parent_id))

    core.db.session.commit()

    # Generate what products exist in which departments and by how much
    for d in departments:
        if random.random() < 0.2:
            # This place focuses on a few products
            for x in range(4):
                factories.DepartmentProductYear(
                    department=d,
                    product=random.choice(products),
                    year=2008
                )
        else:
            # This place is a diverse economy
            for x in range(20):
                factories.DepartmentProductYear(
                    department=d,
                    product=random.choice(products),
                    year=2008
                )

    # Permute data for the following years according to this year.
    for d in departments:
        dpys = models.DepartmentProductYear\
            .query.filter_by(department=d).all()
        for year in range(2009, 2013):
            if random.random() < 0.1:
                delta = random.random() - 0.5
            else:
                delta = 5 * (random.random() - 0.5)

            for dpy in dpys:
                factories.DepartmentProductYear(
                    department=dpy.department,
                    product=dpy.product,
                    year=year,
                    import_value=dpy.import_value * delta,
                    export_value=dpy.export_value * delta
                )

    core.db.session.commit()


def convert_classification(df):

    # Copy in fields and change names appropriately
    new_df = df[["index", "code", "name", "level", "parent_id"]]
    new_df = new_df.rename(columns={
        "index": "id",
        "name": "name_en"
    })

    # Pull in any optional fields
    optional_fields = ["name_es", "name_short_en", "name_short_es",
                       "description_en", "description_es"]

    for field in optional_fields:
        if field in df:
            new_df[field] = df[field]

    return new_df


@manager.command
def import_data():
    """Import data from a data.h5 file."""
    import pandas as pd

    store = pd.HDFStore("./data.h5")

    for key in store.keys():
        metadata = store.get_storer(key).attrs.atlas_metadata
        table_name = metadata["sql_table_name"]

        print("-----------------------------------")
        print(metadata)
        print(key, table_name)

        try:
            table = store[key]

            if key.startswith("/classifications/"):
                table = convert_classification(table)

            if "product_level" in metadata:
                table["level"] = metadata["product_level"]

            table.to_sql(table_name, core.db.engine, index=False,
                         chunksize=10000, if_exists="append")

        except SQLAlchemyError as exc:

            print(exc)

if __name__ == "__main__":
    manager.run()

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, select

engine = create_engine("mysql+pymysql://anmusso:@localhost/citiesdatabase", echo=True, future=True)
metadata = MetaData()


def get_omsa_data():
    with engine.begin() as con:
        table_name = 'omsa'
        my_table = Table(table_name, metadata, autoload_with=engine)
        stmt = select(my_table)
        df = pd.read_sql(stmt, con=con)

    return df


if __name__ == '__main__':
    df = get_omsa_data()



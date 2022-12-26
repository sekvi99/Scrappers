# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html
# https://docs.sqlalchemy.org/en/14/core/reflection.html#sqlalchemy.engine.reflection.Inspector.has_table
# https://stackoverflow.com/questions/31501999/how-to-load-data-into-existing-database-tables-using-sqlalchemy
# https://stackoverflow.com/questions/54491156/validate-json-data-using-python
# https://stackoverflow.com/questions/8793448/how-to-convert-to-a-python-datetime-object-with-json-loads

# ! External moduls that need to be installed in virtual env: SQLAlchemy, dotenv
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, Date, Float, Integer
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError

from data_loading import get_json_of_currency_rates

import json
# ! External moduls that need to be installed in virtual env: jsonschema
from jsonschema import validate
from datetime import datetime

from typing import Callable, Final


def get_database_connection_url() -> str:
    from dotenv import dotenv_values
    return dotenv_values('.env')['CONNECTION_URL']


def build_engine(connection_url: str) -> Engine:
    if not connection_url:
        raise ValueError('Connection string cannot be empty.')

    engine = create_engine(connection_url)
    
    try:
        engine.connect()
        print('Succesfully connected to engine.')
    except SQLAlchemyError as err:
        print(f'Error occured: {err}.')

    return engine


def set_database_connection() -> Engine:
    connection_url = get_database_connection_url()
    return build_engine(connection_url)


def create_currency_rates_table(engine: Engine, table_name: str = 'currency_rate') -> None:
    if not inspect(engine).has_table(table_name):
        metadata = MetaData()

        # ? Exporting this statement as external function.
        currenccy_rates = Table(
            table_name, metadata,
            Column('Id', Integer, primary_key=True, autoincrement=True), 
            Column('Date', Date), 
            Column('EUR', Float),
            Column('USD', Float),
            Column('GBP', Float)
        )

        currenccy_rates.create(engine)
        print(f"Table '{table_name}' succesfully created.")
    else:
        print(f"Table '{table_name}' already exists.")


def validate_json_file(data: str) -> None:
    # ! Check for best method for json validation
    schema = {
        'type': 'object', 
        'properties': {
            'Date': {'type': 'number'}, 
            'EUR': {'type': 'number'}, 
            'USD': {'type': 'number'}, 
            'GBP': {'type': 'number'}, 
        }
    }

    validate(instance=data, schema=schema)


def get_new_records(sql_data: list[dict], new_data: list[dict]) -> list[dict]:
    pass


def load_data(engine: Engine, table_name: str, operation_fn: Callable[[None], None]) -> None:
    if inspect(engine).has_table(table_name):
        metadata = MetaData()
        metadata.reflect(bind=engine)

        data = json.loads(operation_fn(),
            object_hook=lambda d: {key: datetime.fromtimestamp(value / 1e3) 
                if isinstance(value, int) else value for key, value in d.items()})

        # ! Add data validation.
        # validate_json_file(data)
        # ! Function that checks if records are present in table.

        conn = engine.connect()
        conn.execute(metadata.tables[table_name].insert(
            [value for key, value in data.items()]
        ))

        print(f"Succesfully added records to '{table_name}' table.")
    else:
        print(f"Cannot add records, table '{table_name}' doesn't exist.")



def main() -> None:
    engine = set_database_connection()
    TABLE_NAME: Final[str] = 'currency_rates'
    create_currency_rates_table(engine, TABLE_NAME)
    load_data(engine, TABLE_NAME, get_json_of_currency_rates)

if __name__ == '__main__':
    main()


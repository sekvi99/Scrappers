# External moduls that need to be installed in virtual env: SQLAlchemy, dotenv, jsonschema
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError

from typing import Callable

from datetime import datetime
import pandas as pd
import json


def get_database_connection_url() -> str:
    from dotenv import dotenv_values
    return dotenv_values('.env')['CONNECTION_URL']


def build_engine(connection_url: str) -> Engine:
    if not connection_url:
        raise ValueError('Connection string cannot be empty.')

    engine = create_engine(connection_url, echo=False)
    
    try:
        engine.connect()
        print('Succesfully connected to engine.')
    except SQLAlchemyError as err:
        print(f'Error occured: {err}.')

    return engine


def set_connection() -> Engine:
    connection_url = get_database_connection_url()
    return build_engine(connection_url)


def create_table(engine: Engine, table_name: str, table: Table) -> None:
    if not inspect(engine).has_table(table_name):
        table.create(engine)
        print(f"Table '{table_name}' succesfully created.")
    else:
        print(f"Table '{table_name}' already exists.")


def get_new_records(sql_data: pd.DataFrame, scraper_data: pd.DataFrame) -> pd.DataFrame:
    if sql_data.empty:
        return scraper_data

    if scraper_data.empty:
        raise ValueError('Scraper returns no values.')

    sql_data[sql_data.columns[0]] = pd.to_datetime(sql_data[sql_data.columns[0]])
    scraper_data.Date = pd.to_datetime(scraper_data.Date)

    return scraper_data[~scraper_data.Date.isin(sql_data[sql_data.columns[0]])]


def load_data(engine: Engine, table_name: str, operation_fn: Callable[[None], None]) -> None:
    if inspect(engine).has_table(table_name):
        metadata = MetaData()
        metadata.reflect(bind=engine)

        data = json.loads(operation_fn(),
            object_hook=lambda d: {key: datetime.fromtimestamp(value / 1e3).date()
                if isinstance(value, int) else value for key, value in d.items()})
        conn = engine.connect()

        sql_data = pd.DataFrame(element[1:] for element in 
            conn.execute(metadata.tables[table_name].select()))
        scraper_data = pd.DataFrame(value for value in data.values())

        if (new_records := get_new_records(sql_data, scraper_data)).empty:
            print(f'No new records to add.')
        else:
            print(f'Found {len(new_records)} new records.')
            conn.execute(metadata.tables[table_name].insert(new_records.to_dict('records')))
            print('Records added.')
    else:
        print(f"Cannot add records, table '{table_name}' doesn't exist.")

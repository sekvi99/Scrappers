import requests

import pandas as pd

from sqlalchemy import MetaData, Table, Column, Date, Float, Integer


def get_endpoint(currency: str) -> str:
    if not currency:
        raise ValueError('String cannot be empty.')
    return f'https://www.bankier.pl/new-charts/get-data?symbol={currency}PLN&intraday=false&type=area&max_period=true'


def load_and_process_data(currency: str) -> pd.DataFrame:

    def load_data() -> pd.DataFrame:
        response =  requests.request("GET", get_endpoint(currency)).json().get('main')
        return pd.DataFrame(data=response, columns=['Date', f'{currency}']) 

    df = load_data()
    df[df.columns[0]] = df[df.columns[0]].astype('datetime64[ms]')
    df[df.columns[1]] = df[df.columns[1]].astype('float')
    
    return df


def get_json_of_currency_rates(currencies: str = ['EUR', 'USD', 'GBP']) -> str:
    df = pd.DataFrame(columns=['Date'])
    for currency in currencies:
         df = df.merge(load_and_process_data(currency), on='Date', how='outer')
    return df.dropna().reset_index(drop=True).to_json(orient="index")


def create_currency_rates_table(table_name: str) -> Table:
    metadata = MetaData()
    
    return Table(
        table_name, metadata,
        Column('Id', Integer, primary_key=True, autoincrement=True), 
        Column('Date', Date), 
        Column('EUR', Float),
        Column('USD', Float),
        Column('GBP', Float)
    )


def main() -> None:
    get_json_of_currency_rates()

if __name__ == '__main__':
    main()
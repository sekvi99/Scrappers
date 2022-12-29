import pandas as pd
import os
import requests
from typing import Final
from sqlalchemy import MetaData, Table, Column, Date, Float, Integer, String

# ! Path to current working directory
DIR_PATH:Final[str] = os.getcwd()

# ! List of Stock Companies shortcuts
SYMBOLS:Final[list] = list([
    'IBM',
    'TSCO.LON',
    'SHOP.TRT',
    'GPV.TRV',
    'DAI.DEX',
    'RELIANCE.BSE',
])

# ! Mapper for dataframe object
COLUMNS_MAPPER:Final[dict] = dict({
    '1. open':'Open_Price',
    '2. high':'High_Price',
    '3. low' :'Low_Price',
    '4. close':'Close_Price',
    '5. volume':'Volume'
})

def download_data(symbol: str) -> pd.DataFrame:
    """
    Function for downloading data from passsed symbol, that generaters API ENDPOINTS
    """
    def link_generator() -> str:
        return f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey=demo'
    
    respone = requests.get(link_generator())
    data = (respone.json())['Time Series (Daily)']
    df = pd.DataFrame(data=data)
    df = df.T
    df = df.rename(columns = COLUMNS_MAPPER)
    df = df.reset_index(drop=False)
    df['Symbol'] = symbol
    df['index'] = pd.to_datetime(df['index'])
    df = df.rename(columns = {'index':'Date'}) 
    for col in list(df.columns)[1:-1]:
        df[col] = df[col].astype(float)
        df[col] = df[col].apply(lambda x: round(x, 2))
    return df

def get_every_api_endpoint_as_df() -> pd.DataFrame:
    """
    Function to run download for every single one endpoint declared in symbols.
    """
    merged_df = pd.concat([download_data(symbol) for symbol in SYMBOLS])
    merged_df = merged_df.reset_index(drop=True)
    return merged_df

def create_stock_exchange_table(table_name: str) -> Table:
    metadata = MetaData()
    
    return Table(
        table_name, metadata,
        Column('Id', Integer, primary_key=True, autoincrement=True), 
        Column('Date', Date), 
        Column('Open_Price', Float),
        Column('High_Price', Float),
        Column('Low_Price', Float),
        Column('Close_Price', Float),
        Column('Volume', Float),
        Column('Symbol', String)
    )

def get_json_of_stock_exchange() -> str:
    """
    Function that saves data in JSON_DUMP directory.
    """
    from datetime import datetime
    df = get_every_api_endpoint_as_df()
    # now = (datetime.now()).strftime('%Y-%m-%d')
    # json_dir = os.path.join(DIR_PATH, r'JSON_DUMP')
    return df.to_json( orient='index')

def main() -> None:
    get_json_of_stock_exchange()

if __name__ == '__main__':
    main()
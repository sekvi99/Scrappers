import pandas as pd
import numpy as np
import pyodbc
import requests
from datetime import datetime
import os

"""
Deklaracja stałych:
CURRENCIES => Lista skróconych nazw walut
LOCAL_PATH => Ścieżka do folderu, w którym znajduje się skrypt
"""
CURRENCIES = ['EUR', 'USD', 'GBP']
LOCAL_PATH = os.getcwd()

"""
Funkcja generuje string, który reprezentuje endpoint w API,
z którego pobieramy dane dla poszczególnych walut
"""
def get_endpoint(currency: str) -> str:
    return f'https://www.bankier.pl/new-charts/get-data?symbol={currency}PLN&intraday=false&type=area&max_period=true'

"""
Funkcja przetwarza otrzymany output z API,
przekształca go w odpowiednio sformatowaną ramkę danych w postaci
|Data|Wartość dla danej waluty w PLN|Typ Waluty|
"""
def preporcess_api_output(url: str, currency:str) -> pd.DataFrame:
    response = ((requests.request("GET", url)).json()).get('main')
    
    dates = list()
    values = list()
    for frame in response:
        dates.append(frame[0])
        values.append(frame[1])
    df = pd.DataFrame(data=dict({
        'Date': dates,
        'Value':values,
        'Currency':currency,
    }))
    df['Date'] = df['Date'].values.astype(dtype='datetime64[ms]')
    df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d'))
    df['Value'] = df['Value'].astype(float)
    return df

"""
Funkcja, która dla każdej waluty podanej w CURRENCIES,
wywołuje utworzenie connector'a do API oraz wywołanie pobrania i przetwarzania danych
Na końcu funkcja zapisuje dane do formatu .json
"""
def download_data() -> None:
    df_list = list()
    for currency in CURRENCIES:
        print(f'Pobieranie danych: {currency}')
        currency_str = get_endpoint(currency)
        currency_df = preporcess_api_output(currency_str, currency)
        df_list.append(currency_df)
    merged = pd.concat(df_list)
    merged = merged.reset_index(drop = True)
    merged = merged.sort_values(by='Date')

    now = (datetime.now()).strftime('%Y-%m-%d')
    merged.to_json(os.path.join(os.path.join(LOCAL_PATH, r'JSON_DUMP'), f'currency-rates-{now}.json'), orient='records')

"""
Funkcja bierze plik .json znajdujący się w tym folderze co skrypt
i na jego podstawie dodaje rekordy do bazy
"""
def json_uploader() -> None:
    df_list = list()
    json_dir = os.path.join(LOCAL_PATH, r'JSON_DUMP')
    json_files = os.listdir(json_dir)
    for file in json_files:
        df_list.append(pd.read_json(file))
    df = pd.concat(df_list)
    df = df.drop_duplicates()
    print(df)

"""
Wyowłanie funkcji main
"""
def main() -> None:
    #download_data()
    json_uploader()


if __name__ == '__main__':
    main()
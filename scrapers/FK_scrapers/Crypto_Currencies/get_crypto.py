import pandas as pd
from pycoingecko import CoinGeckoAPI
from typing import Final
import requests
import time
import datetime

# ! Symbols of API Endpoints for given crypto-currency
CURRENCIES:Final[list] = list([
    'bitcoin',
    'ethereum',
    'tether',
    'usdc',
    'bnb',
    'xrp',
    'busd',
    'dogecoin',
    'cardano',
])

# ! Mapper => api_symbol to currency name
SYMBOL_TO_NAME_MAPPER:Final[dict] = dict({
    'bitcoin' : 'Bitcoin',
    'ethereum' : 'Ethereum',
    'tether' : 'Tether',
    'usdc' : 'USD Coin',
    'bnb' : 'OEC Binance Coin',
    'xrp' : 'Heco-Peg XRP',
    'busd' : 'Binance USD',
    'dogecoin' : 'Buff Doge Coin',
    'cardano' : 'Cardano',
})

def convert_date(date_str: str) -> int:
    """
    Function that converts string date into timestamp
    """
    return time.mktime(datetime.datetime.strptime(date_str, "%Y-%m-%d").timetuple())

def get_given_coin(symbol: str) -> str:
    """
    Function that returns a string endpoint for given currency symbol.
    """
    str_date_to = (datetime.datetime.now()).strftime('%Y-%m-%d')
    str_date_from = '2013-01-01'

    date_from = convert_date(str_date_from)
    date_to = convert_date(str_date_to)

    def generate_link() -> str:
        return f'https://api.coingecko.com/api/v3/coins/{symbol}/market_chart/range?vs_currency=usd&from={date_from}&to={date_to}'

    return generate_link()


def convert_api_endpoint_to_dataframe(url: str, coin_name: str) -> pd.DataFrame:
    """
    Function takes endpoint and convert it to data in dataframe.
    """
    day_divider = 86400000
    response = requests.get(url = url)
    data = response.json() 
    if 'prices' in list(data.keys()):
        data = data['prices'] # ! List in form [timestamp, value]
        dates, values = map(list, zip(*data))
        df = pd.DataFrame(data=dict({
            'Date':dates,
            'Value':values,
            'Coin_Symbol':coin_name
        }))
        df['Date'] = (df['Date']/day_divider).values.astype(dtype='datetime64[D]')
        df['Value'] = df['Value'].astype(float)

        return df
    

def get_merged_dataframe() -> pd.DataFrame:
    """
    Function that returns a merged dataframe for every declared coin.
    """
    merged_df = pd.concat([convert_api_endpoint_to_dataframe(get_given_coin(coin), SYMBOL_TO_NAME_MAPPER.get(coin)) for coin in CURRENCIES])
    merged_df = merged_df.reset_index(drop=True)
    return merged_df

def get_crypto_currency_json() -> str:
    df = get_merged_dataframe()
    return df.to_json(orient='index')


def main() -> None:
    get_crypto_currency_json()

if __name__ == '__main__':
    main()
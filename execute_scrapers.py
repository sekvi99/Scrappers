from database_handling.handlers import set_connection, create_table, load_data

from typing import Callable
from sqlalchemy import Table


def execute_scraper(table_name: str, table_fn: Callable[[str], Table], \
        scraper_fn: Callable[[list[str]], str]) -> None:
    engine = set_connection()

    table_schema = table_fn(table_name)
    create_table(engine, table_name, table_schema)

    load_data(engine, table_name, scraper_fn)


def execute_all_scrapers() -> None:
    print(f'\nLoading currency rates:')
    from scrapers.WK_scrapers.currency_rates.currency_rates_scraper import \
        get_json_of_currency_rates, create_currency_rates_table
    execute_scraper('currency_rates', create_currency_rates_table, get_json_of_currency_rates)

    print(f'\nLoading stock exchange:')
    from scrapers.FK_scrapers.Stock_Exchange.stock_exchange import \
        get_json_of_stock_exchange, create_stock_exchange_table
    execute_scraper('stock_exchange', create_stock_exchange_table, get_json_of_stock_exchange)

    print(f'\nLoading crypto currencies:')
    from scrapers.FK_scrapers.Crypto_Currencies.get_crypto import \
        get_crypto_currency_json, create_crypto_table
    execute_scraper('crypto_currencies', create_crypto_table, get_crypto_currency_json)


def main() -> None:
    execute_all_scrapers()


if __name__ == '__main__':
    main()
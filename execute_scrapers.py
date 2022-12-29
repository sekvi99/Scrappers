from database_handling.handlers import set_connection, create_table, load_data


def execute_currency_rates_scraper(table_name: str = 'currency_rates') -> None:
    from scrapers.WK_scrapers.currency_rates.currency_rates_scraper import \
        get_json_of_currency_rates, create_currency_rates_table

    engine = set_connection()

    table_schema = create_currency_rates_table(table_name)
    create_table(engine, table_name, table_schema)

    load_data(engine, table_name, get_json_of_currency_rates)


def exceture_stock_exchange(table_name: str = 'stock_exchange_test_test') -> None:
    from scrapers.FK_scrapers.Stock_Exchange.stock_exchange import \
        get_json_of_stock_exchange, create_stock_exchange_table

    engine = set_connection()

    table_schema = create_stock_exchange_table(table_name)
    create_table(engine, table_name, table_schema)

    load_data(engine, table_name, get_json_of_stock_exchange)

def main() -> None:
    #execute_currency_rates_scraper()
    exceture_stock_exchange()

if __name__ == '__main__':
    main()
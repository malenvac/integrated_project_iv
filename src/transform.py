from collections import namedtuple
from enum import Enum
from typing import Callable, Dict, List
import os 

import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text 

from src.config import QUERIES_ROOT_PATH

QueryResult = namedtuple("QueryResult", ["query", "result"])


class QueryEnum(Enum):
    DELIVERY_DATE_DIFFERECE = "delivery_date_difference"
    GLOBAL_AMMOUNT_ORDER_STATUS = "global_ammount_order_status"
    REVENUE_BY_MONTH_YEAR = "revenue_by_month_year"
    REVENUE_PER_STATE = "revenue_per_state"
    TOP_10_LEAST_REVENUE_CATEGORIES = "top_10_least_revenue_categories"
    TOP_10_REVENUE_CATEGORIES = "top_10_revenue_categories"
    REAL_VS_ESTIMATED_DELIVERED_TIME = "real_vs_estimated_delivered_time"
    ORDERS_PER_DAY_AND_HOLIDAYS_2017 = "orders_per_day_and_holidays_2017"
    GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP = "get_freight_value_weight_relationship"


def read_query(query_name: str) -> str:

    query_file_path = os.path.join(QUERIES_ROOT_PATH, f"{query_name}.sql")
    try:
        with open(query_file_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        print(f"❌ Error: Query file not found at {query_file_path}")
        raise


def execute_query_to_dataframe(sql_query: str, engine: Engine) -> DataFrame:
    """Executes SQL query using SQLAlchemy engine and returns a Pandas DataFrame."""
    with engine.connect() as connection:
        result_proxy = connection.execute(text(sql_query))
        data = result_proxy.fetchall()
        columns = result_proxy.keys()
    return pd.DataFrame(data, columns=columns)


def query_delivery_date_difference(engine: Engine) -> QueryResult:
    query_name = QueryEnum.DELIVERY_DATE_DIFFERECE.value
    query = read_query(query_name)
    print(f"🧾 Ejecutando consulta: {query_name}")
    result_df = execute_query_to_dataframe(query, engine)
    return QueryResult(query=query_name, result=result_df)


def query_global_ammount_order_status(engine: Engine) -> QueryResult:
    query_name = QueryEnum.GLOBAL_AMMOUNT_ORDER_STATUS.value
    query = read_query(query_name)
    print(f"🧾 Ejecutando consulta: {query_name}")
    result_df = execute_query_to_dataframe(query, engine)
    return QueryResult(query=query_name, result=result_df)


def query_revenue_by_month_year(engine: Engine) -> QueryResult:
    query_name = QueryEnum.REVENUE_BY_MONTH_YEAR.value
    query = read_query(query_name)
    print(f"🧾 Ejecutando consulta: {query_name}")
    result_df = execute_query_to_dataframe(query, engine)
    return QueryResult(query=query_name, result=result_df)


def query_revenue_per_state(engine: Engine) -> QueryResult:
    query_name = QueryEnum.REVENUE_PER_STATE.value
    query = read_query(query_name)
    print(f"🧾 Ejecutando consulta: {query_name}")
    result_df = execute_query_to_dataframe(query, engine)
    return QueryResult(query=query_name, result=result_df)


def query_top_10_least_revenue_categories(engine: Engine) -> QueryResult:
    query_name = QueryEnum.TOP_10_LEAST_REVENUE_CATEGORIES.value
    query = read_query(query_name)
    print(f"🧾 Ejecutando consulta: {query_name}")
    result_df = execute_query_to_dataframe(query, engine)
    return QueryResult(query=query_name, result=result_df)


def query_top_10_revenue_categories(engine: Engine) -> QueryResult:
    query_name = QueryEnum.TOP_10_REVENUE_CATEGORIES.value
    query = read_query(query_name)
    print(f"🧾 Ejecutando consulta: {query_name}")
    result_df = execute_query_to_dataframe(query, engine)
    return QueryResult(query=query_name, result=result_df)


def query_real_vs_estimated_delivered_time(engine: Engine) -> QueryResult:
    query_name = QueryEnum.REAL_VS_ESTIMATED_DELIVERED_TIME.value
    query = read_query(query_name)
    print(f"🧾 Ejecutando consulta: {query_name}")
    result_df = execute_query_to_dataframe(query, engine)
    return QueryResult(query=query_name, result=result_df)


def query_freight_value_weight_relationship(engine: Engine) -> QueryResult:
    query_name = QueryEnum.GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP.value
    print(f"📦 Ejecutando transformación: {query_name}")

    orders = execute_query_to_dataframe("SELECT * FROM olist_orders", engine)
    items = execute_query_to_dataframe("SELECT * FROM olist_order_items", engine)
    products = execute_query_to_dataframe("SELECT * FROM olist_products", engine)

    data = items.merge(orders, on='order_id').merge(products, on='product_id')
    delivered = data[data['order_status'] == 'delivered']

    aggregations = delivered.groupby('order_id').agg({
        'freight_value': 'sum',
        'product_weight_g': 'sum'
    }).reset_index()

    return QueryResult(query=query_name, result=aggregations)


def query_orders_per_day_and_holidays_2017(engine: Engine) -> QueryResult:
    query_name = QueryEnum.ORDERS_PER_DAY_AND_HOLIDAYS_2017.value
    print(f"📅 Ejecutando transformación: {query_name}")

    holidays = execute_query_to_dataframe("SELECT * FROM public_holidays", engine)
    orders = execute_query_to_dataframe("SELECT * FROM olist_orders", engine)

    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])
    filtered_dates = orders[orders["order_purchase_timestamp"].dt.year == 2017]

    order_purchase_ammount_per_date = filtered_dates.groupby(
        filtered_dates["order_purchase_timestamp"].dt.date
    ).size().reset_index(name='order_count')

    order_purchase_ammount_per_date = order_purchase_ammount_per_date.rename(
        columns={"order_purchase_timestamp": "date"}
    )

    holidays['date'] = pd.to_datetime(holidays['date']).dt.date

    result_df = order_purchase_ammount_per_date
    result_df['holiday'] = result_df['date'].isin(holidays['date'])

    return QueryResult(query=query_name, result=result_df)


# get_all_queries remains the same
def get_all_queries() -> List[Callable[[Engine], QueryResult]]:
    return [
        query_delivery_date_difference,
        query_global_ammount_order_status,
        query_revenue_by_month_year,
        query_revenue_per_state,
        query_top_10_least_revenue_categories,
        query_top_10_revenue_categories,
        query_real_vs_estimated_delivered_time,
        query_orders_per_day_and_holidays_2017,
        query_freight_value_weight_relationship,
    ]


def run_queries(engine: Engine) -> Dict[str, DataFrame]:
    query_results = {}
    errors_occurred = []
    for query_func in get_all_queries():
        print(f"\n🚀 Ejecutando: {query_func.__name__}")
        try:
            query_result = query_func(engine)
            print(f"✅ Completado: {query_result.query} con {len(query_result.result)} filas")
            query_results[query_result.query] = query_result.result
        except Exception as e:
            error_msg = f"❌ Error en {query_func.__name__}: {str(e)}"
            print(error_msg)
            errors_occurred.append(error_msg)

    return query_results
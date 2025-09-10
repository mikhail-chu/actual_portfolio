"""
# Wildberries: DAG для загрузки и обработки заказов и продаж

**Описание:**
DAG ежедневно получает данные о заказах и продажах с Wildberries через API, агрегирует их и загружает в базу данных.

До загрузки в БД отчет также отправляется в Google Sheets

Неизвестные SKU загружаются в БД в отдельную таблицу

**Расписание:**
- **Запуск:** ежедневно в 10:10
- **Catchup:** выключен

**Основные задачи:**
1. `fetch_report` — Получение отчёта по заказам и продажам через API Wildberries
2. `fetch_products` — Получение справочника товаров (SKU, штрихкоды)
3. `transform_data` — Агрегация и объединение данных, подготовка к загрузке
4. `upload_report_to_sheets` — Выгружает отчет по заказам и продажам в Google Sheets
5. `load_data` — Загрузка данных в базу, логирование невалидных SKU
"""

import pandas as pd
import datetime
import logging

from pytz import timezone
from typing import Dict, Any

from airflow.models import Variable
from airflow.decorators import dag, task

from lib_custom.google_sheets import (
    GoogleSheetsClient,
    SkuCatalogGoogleSheets,
    UnknownSkuOrdersSheets
)
from lib_custom.api.db import DBManagerAPI
from lib_custom.api.mp.wb import WBAPIAnalytics, WBAPIStatistics
from lib_custom.api.mp.wb import WBAPIContent

logger = logging.getLogger(__name__)
tg_logger = logging.getLogger("airflow.tg")

default_config = {
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=timezone("Europe/Moscow")),
    "catchup": False,
    "doc_md": __doc__,
}

default_params = {
    "SPREADSHEET_ID": "1ZrBzs_L8g1GP4BvScBJ3AZxDsxHx7rKWqdP8uEdJXgs",
    "DATE_OFFSET": 1,
    "DATE_PERIOD": 45,
    "USE_PROD_DB": True,
    "USE_BARCODES": False,
}
configs = {
    "project1": {
        "schedule": "0 9 * * *",
        "dag_id": "orders_sales_project1_wb",
        "tags": ["churilov", "project1", "mp", "wb", "api"],
        "params": {
            "API_KEY_VARNAME": "project1_wb_api_key",
            "WORKSHEET_NAME": "project1",
            **default_params,
        },
        **default_config,
    },
    "project2": {
        "schedule": "10 9 * * *",
        "dag_id": "orders_sales_project2_wb",
        "tags": ["churilov", "project2", "mp", "wb", "api"],
        "params": {
            "API_KEY_VARNAME": "project2_wb_api_key",
            "WORKSHEET_NAME": "project2",
            **default_params,
        },
        **default_config,
    },
    "project3": {
        "schedule": "20 9 * * *",
        "dag_id": "orders_sales_project3_wb",
        "tags": ["churilov", "project3", "mp", "wb", "api"],
        "params": {
            "API_KEY_VARNAME": "project3_wb_api_key",
            "WORKSHEET_NAME": "project3",
            **default_params,
        },
        **default_config,
    },
    "project4": {
        "schedule": "30 9 * * *",
        "dag_id": "orders_sales_project4_wb",
        "tags": ["churilov", "project4", "mp", "wb", "api"],
        "params": {
            "API_KEY_VARNAME": "project4_wb_api_key_new",
            "WORKSHEET_NAME": "project4",
            **default_params,
            "USE_BARCODES": True,
        },
        **default_config,
    },
    "project5": {
        "schedule": "40 9 * * *",
        "dag_id": "orders_sales_project5_wb",
        "tags": ["churilov", "project5", "mp", "wb", "api"],
        "params": {
            "API_KEY_VARNAME": "project5_wb_api_key",
            "WORKSHEET_NAME": "project5",
            **default_params,
        },
        **default_config,
    },
    "project6": {
        "schedule": "50 9 * * *",
        "dag_id": "orders_sales_project6_wb",
        "tags": ["churilov", "project6", "mp", "wb", "api"],
        "params": {
            "API_KEY_VARNAME": "fnmn_wb_api_key",
            "WORKSHEET_NAME": "project6",
            **default_params,
        },
        **default_config,
    },
}


def get_db_api(params):
    db_api = DBManagerAPI(
        base_url=(
            Variable.get("db_api_url_prod")
            if params["USE_PROD_DB"]
            else Variable.get("db_api_url_local")
        ),
        token=Variable.get("db_api_token"),
        timeout=60,
        logger=logger,
    )
    return db_api


for name, config in configs.items():

    @dag(**config)
    def generate_dag():

        @task
        def fetch_orders_sales_report(params: dict):
            """
            Получает отчёт по заказам и продажам с Wildberries API за указанный период.
            Возвращает DataFrame с деталями заказов.
            :param params: параметры DAG (содержат ключ API, период и т.д.)
            :return: pd.DataFrame с заказами
            """
            token = Variable.get(params["API_KEY_VARNAME"])

            wb_api = WBAPIAnalytics(token=token, logger=logger)

            output_df: pd.DataFrame = wb_api.fetch_orders_sales_report(
                date_offset=params["DATE_OFFSET"], date_period=params["DATE_PERIOD"]
            )

            output_df = output_df.loc[
                :,
                [
                    "nmID",
                    "dt",
                    "ordersCount",
                    "ordersSumRub",
                    "buyoutsCount",
                    "buyoutsSumRub",
                ],
            ]
            output_df = output_df.rename(
                columns={
                    "nmID": "product_id",
                    "dt": "order_date",
                    "ordersCount": "orders_amount",
                    "ordersSumRub": "orders_revenue",
                    "buyoutsCount": "sales_amount",
                    "buyoutsSumRub": "sales_revenue",
                }
            )
            output_df["order_date"] = (output_df["order_date"].dt.date).astype(str)
            output_df["product_id"] = output_df["product_id"].astype(str)
            output_df = output_df.assign(channel_shortname="WB")
            return output_df

        @task
        def fetch_sales_data(params: dict):
            token = Variable.get(params["API_KEY_VARNAME"])

            wb_api = WBAPIStatistics(token=token, logger=logger)

            output_df: pd.DataFrame = wb_api.fetch_plain_sales(
                date_offset=params["DATE_OFFSET"],
                date_period=params["DATE_PERIOD"]
            )

            output_df = output_df.loc[
                :,
                [
                    "order_date",
                    "product_id",
                    "order_cost",
                    "sale_date",
                    "products_count"
                ],
            ]
            output_df["order_date"] = (output_df["order_date"].dt.date).astype(str)
            output_df["sale_date"] = (output_df["sale_date"].dt.date).astype(str)
            output_df["product_id"] = output_df["product_id"].astype(str)
            output_df["sale_cost"] = (~output_df["sale_date"].isna()).astype(int) * output_df["order_cost"]

            return output_df

        @task
        def fetch_products(params: dict):
            """
            Получает справочник товаров (SKU и штрихкоды) через API Wildberries.
            :param params: параметры DAG (содержат ключ API)
            :return: pd.DataFrame с товарами
            """

            token = Variable.get(params["API_KEY_VARNAME"])
            project = params["WORKSHEET_NAME"].lower()
            wb_api = WBAPIContent(token=token, logger=logger)

            products_df = wb_api.fetch_full_products(project_name=project)
            if params["USE_BARCODES"]:
                columns_dict = {
                    "mp_id": "product_id",
                    "barcode": "product_sku",
                    "product_name": "product_name",
                    "project": "project_name",
                }
            else:
                columns_dict = {
                    "mp_id": "product_id",
                    "mp_sku": "product_sku",
                    "product_name": "product_name",
                    "project": "project_name",
                }

            output_df: pd.DataFrame = products_df.loc[:, columns_dict.keys()]
            output_df = output_df.rename(columns=columns_dict)

            return output_df

        @task(multiple_outputs=True)
        def transform_orders_data(df_report: pd.DataFrame, df_products: pd.DataFrame):
            """
            Агрегирует и объединяет данные заказов и справочника товаров.
            Группирует по дате и SKU, рассчитывает метрики заказов и продаж.
            :param df_report: DataFrame с заказами
            :param df_products: DataFrame с товарами
            :return: pd.DataFrame, готовый к загрузке в БД
            """

            df = pd.merge(df_report, df_products, "left", ["product_id"])

            without_barcode = df[df["product_sku"].isnull()]
            result = df[df["product_sku"].notnull()]

            return {"result": result, "without_barcode": without_barcode}

        @task(multiple_outputs=True)
        def transform_sales_data(df_report: pd.DataFrame, df_products: pd.DataFrame):
            """
            Агрегирует и объединяет данные заказов и справочника товаров.
            Группирует по дате и SKU, рассчитывает метрики заказов и продаж.
            :param df_report: DataFrame с заказами
            :param df_products: DataFrame с товарами
            :return: pd.DataFrame, готовый к загрузке в БД
            """
            df = pd.merge(df_report, df_products, "left", ["product_id"])
            df = (
                df.groupby(["sale_date", "product_sku", "product_name", "project_name"])
                .agg(
                    sales_amount=("products_count", "sum"),
                    sales_revenue=("sale_cost", "sum"),
                )
                .reset_index()
            )
            output_df = df.loc[
                :,
                [
                    "sale_date",
                    "product_sku",
                    "sales_amount",
                    "sales_revenue",
                    "product_name",
                    "project_name"
                ],
            ]
            output_df["channel_shortname"] = "WB"

            without_barcode = output_df[output_df["product_sku"].isnull()]
            result = output_df[output_df["product_sku"].notnull()]

            return {"result": result, "without_barcode": without_barcode}

        @task
        def upload_report_to_sheets(df: pd.DataFrame, params: dict):
            output_df = df[
                [
                    "order_date",
                    "product_sku",
                    "orders_amount",
                    "orders_revenue",
                    "sales_amount",
                    "sales_revenue",
                ]
            ]
            output_df["order_date"] = pd.to_datetime(output_df["order_date"])
            gs_client = GoogleSheetsClient()
            gs_client.update_data(df=output_df, cell_range="A2:F", params=params, clear_range=True)

        @task
        def load_orders_data(df: pd.DataFrame, params: dict):
            """
            Загружает агрегированные данные в базу данных с помощью batch upsert.
            Логирует невалидные SKU в Telegram, если такие есть.
            :param df: DataFrame для загрузки
            :param sql_query: SQL-запрос для upsert
            """
            for_upload = df.loc[
                :,
                [
                    "order_date",
                    "product_sku",
                    "product_name",
                    "project_name",
                    "channel_shortname",
                    "orders_amount",
                    "orders_revenue",
                    "sales_amount",
                    "sales_revenue",
                ],
            ]
            for_upload["use_barcodes"] = params["USE_BARCODES"]
            duplicates = for_upload.duplicated(
                subset=["order_date", "product_sku", "channel_shortname"], keep=False
            )
            duplicate_rows = for_upload[duplicates]
            if duplicate_rows.shape[0] > 0:
                logger.info("DUPLICATES")
                logger.info(duplicate_rows.to_dict())
                return

            db_api = get_db_api(params)
            result = db_api.upsert_mp_orders(for_upload.to_dict(orient="records"))
            logger.info(result)

            return result

        @task
        def load_sales_data(df: pd.DataFrame, params: dict):
            """
            Загружает агрегированные данные в базу данных с помощью batch upsert.
            Логирует невалидные SKU в Telegram, если такие есть.
            :param df: DataFrame для загрузки
            :param sql_query: SQL-запрос для upsert
            """
            for_upload = df.loc[
                :,
                [
                    "sale_date",
                    "product_sku",
                    "product_name",
                    "project_name",
                    "channel_shortname",
                    "sales_amount",
                    "sales_revenue",
                ],
            ]
            for_upload["use_barcodes"] = params["USE_BARCODES"]
            duplicates = for_upload.duplicated(
                subset=["sale_date", "product_sku", "channel_shortname"], keep=False
            )
            duplicate_rows = for_upload[duplicates]
            if duplicate_rows.shape[0] > 0:
                logger.info("DUPLICATES")
                logger.info(duplicate_rows.to_dict())
                return

            db_api = get_db_api(params)
            result = db_api.upsert_mp_sales(for_upload.to_dict(orient="records"))
            logger.info(result)

            return result

        @task
        def upload_unknown_skus_stats(
            df: pd.DataFrame,
            orders_upload_result: Dict[str, Any],
            params: Dict[str, Any]
        ):
            if df.empty:
                logger.info("Dataframe with orders and sales is empty")
                return

            gs_client = SkuCatalogGoogleSheets()

            gs_client.upload_sku_stats(
                project_name=params["WORKSHEET_NAME"].lower(),
                channel_name=df["channel_shortname"].iloc[0],
                sku_stats=orders_upload_result["skus"]
            )

            missing_skus = pd.DataFrame(orders_upload_result["skus"].get("missing_skus"))

            if not missing_skus.empty:
                unique_skus = list(missing_skus["sku"].unique())

                orders_with_missing_skus = df[df["product_sku"].isin(unique_skus)]
                orders_with_missing_skus = orders_with_missing_skus.loc[
                    :,
                    [
                        "order_date",
                        "product_sku",
                        "product_name",
                        "orders_amount",
                        "orders_revenue",
                        "sales_amount",
                        "sales_revenue"
                    ]
                ]
                orders_gs_client = UnknownSkuOrdersSheets()

                orders_gs_client.upload_orders_sales(
                    project_name=params["WORKSHEET_NAME"].lower(),
                    channel_name=df["channel_shortname"].iloc[0],
                    orders_stats=orders_with_missing_skus
                )

        orders_sales_report = fetch_orders_sales_report()
        sales_data = fetch_sales_data()
        products = fetch_products()
        orders_transformed_result = transform_orders_data(orders_sales_report, products)
        sales_transformed_result = transform_sales_data(sales_data, products)
        upload_report_to_sheets(orders_transformed_result["result"])

        orders_upload_result = load_orders_data(
            orders_transformed_result["result"]
        )
        _ = load_sales_data(
            sales_transformed_result["result"]
        )
        upload_unknown_skus_stats(
            orders_transformed_result["result"],
            orders_upload_result=orders_upload_result
        )

    generate_dag()

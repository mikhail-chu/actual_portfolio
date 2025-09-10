"""
# Huntflow podbor & SLA DAG

This dag does api load into DB "huntflow"
## Schedule

- **Frequency**: Runs daily at 4:20.
- **Catch Up**: False

## Tasks
"""
import datetime
from pytz import timezone
import pandas as pd
from io import StringIO

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql as SQL
from psycopg2.extras import execute_values

import logging


from lib_custom.huntflow_api import (
    authorize,
    get_vacancies,
    get_applicants,
    get_applicant_logs,
    get_vacancy_logs,
    get_dictionaries
)
logger = logging.getLogger(__name__)


@dag(
    dag_id="huntflow_load",
    schedule="20 4 * * *",
    start_date=datetime.datetime(2025, 1, 1, tzinfo=timezone("Europe/Moscow")),
    catchup=False,
    template_searchpath=["---------"],
    tags=["huntflow", "churilov"],
)
def huntflow_load():

    @task()
    def step_authorize():
        account_id, headers = authorize()
        return {"account_id": account_id, "headers": headers}

    @task()
    def step_get_dicts(auth):
        return get_dictionaries(auth["account_id"], auth["headers"])

    @task()
    def step_get_vacancies_json(auth_dicts):
        auth = auth_dicts["auth"]
        dicts = auth_dicts["dicts"]

        df = get_vacancies(auth["account_id"], auth["headers"], dicts)
        df = df.rename(columns={
            "id": "vacancy_id",
            "account_division": "division",
            "state": "current_state"
        })[
            ["vacancy_id", "position", "division", "money", "priority", "hidden",
                "current_state", "created", "agreement_top", "recruiter"]
        ]
        return df.to_json(orient="records")

    @task()
    def step_get_vacancy_ids(json_vacancies: str):
        df = pd.read_json(StringIO(json_vacancies))
        return df["vacancy_id"].dropna().unique().tolist()

    @task()
    def step_get_vacancy_logs(auth_dicts, vacancy_ids):
        auth = auth_dicts["auth"]
        dicts = auth_dicts["dicts"]

        df_logs = get_vacancy_logs(auth["account_id"], auth["headers"], vacancy_ids, dicts)

        df_logs = df_logs.rename(columns={
            "vac_id": "vacancy_id",
            "created": "changed_at"
        })[
            ["vacancy_id", "state", "changed_at", "account_vacancy_close_reason"]
        ]

        return df_logs.to_json(orient="records")

    @task()
    def step_get_applicants_json(auth_dicts):
        auth = auth_dicts["auth"]
        dicts = auth_dicts["dicts"]

        df = get_applicants(auth["account_id"], auth["headers"], dicts)
        df["full_name"] = df[["first_name", "last_name", "middle_name"]].fillna("").agg(" ".join, axis=1).str.strip()
        df = df.rename(columns={
            "id": "applicant_id",
            "money": "expected_money"
            })[
            ["applicant_id", "full_name", "expected_money", "phone"]
        ]
        return df.to_json(orient="records")

    @task()
    def step_get_applicant_ids(json_applicants: str):
        df = pd.read_json(StringIO(json_applicants))
        return df["applicant_id"].dropna().unique().tolist()

    @task()
    def step_get_applicant_logs(auth_dicts, applicant_ids):
        auth = auth_dicts["auth"]
        dicts = auth_dicts["dicts"]

        df_logs = get_applicant_logs(auth["account_id"], auth["headers"], applicant_ids, dicts)

        df_logs = df_logs.rename(columns={
            "vacancy": "vacancy_id",
            "created": "changed_at"
        })[
            ["vacancy_id", "applicant_id", "changed_at", "type", "status"]
        ]

        return df_logs.to_json(orient="records")

    @task()
    def prepare_sql_query(table: str) -> str:
        template_path = f"/opt/airflow/dags/sql_templates/huntflow_db/upsert_{table}.sql"
        with open(template_path) as file:
            sql_template = SQL.SQL(file.read())

        pg_hook = PostgresHook("postgres_hr")
        conn = pg_hook.get_conn()
        rendered_sql = sql_template.format(SCHEMA_NAME=SQL.Identifier("huntflow")).as_string(conn)
        conn.close()
        return rendered_sql

    @task()
    def load_data(json_data: str, sql_query: str):
        try:
            # Чтение JSON
            if isinstance(json_data, str):
                json_data = StringIO(json_data)
            df = pd.read_json(json_data)

            if df.empty:
                logger.warning("DataFrame пустой, загрузка пропущена.")
                return

            logger.info("Типы колонок:\n%s", df.dtypes)
            logger.info("Первые строки:\n%s", df.head(3).to_dict(orient="records"))

            # Обработка дат
            for col in df.columns:
                if any(substr in col for substr in ("date", "created", "changed")):
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except Exception as e:
                        logger.warning(f"Ошибка при преобразовании даты в колонке {col}: {e}")

            id_cols = ["vacancy_id", "applicant_id"]
            for col in id_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            existing_cols = [col for col in id_cols if col in df.columns]
            if existing_cols:
                before = len(df)
                df = df.dropna(subset=existing_cols)
                dropped = before - len(df)
                if dropped > 0:
                    logger.warning(f"Удалено {dropped} строк с NaN в колонках: {existing_cols}")

            # Приведение к int (уже без NaN)
            for col in existing_cols:
                df[col] = df[col].astype(int)

            if df.empty:
                logger.warning("После фильтрации данных не осталось.")
                return

            # Загрузка в БД
            pg_hook = PostgresHook("postgres_hr")
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            execute_values(cursor, sql_query, list(df.itertuples(index=False, name=None)))
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Загружено строк: {len(df)}")

        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}", exc_info=True)
            raise

    # Авторизация и словари
    auth = step_authorize()
    dicts = step_get_dicts(auth)

    auth_dicts = {
        "auth": auth,
        "dicts": dicts
    }

    # Получение вакансий и кандидатов
    json_vacancies = step_get_vacancies_json(auth_dicts)
    json_applicants = step_get_applicants_json(auth_dicts)

    # Извлечение ID
    vacancy_ids = step_get_vacancy_ids(json_vacancies)
    applicant_ids = step_get_applicant_ids(json_applicants)

    # Выгрузка логов
    vacancy_logs = step_get_vacancy_logs(auth_dicts, vacancy_ids)
    applicant_logs = step_get_applicant_logs(auth_dicts, applicant_ids)

    # SQL-шаблоны
    sql_applicants = prepare_sql_query.override(task_id="prepare_applicants")("applicants")
    sql_vacancies = prepare_sql_query.override(task_id="prepare_vacancies")("vacancies")
    sql_app_logs = prepare_sql_query.override(task_id="prepare_applicants_logs")("applicants_logs")
    sql_vac_logs = prepare_sql_query.override(task_id="prepare_vacancies_logs")("vacancies_logs")

    # Загрузка данных
    load_data.override(task_id="load_applicants")(json_applicants, sql_applicants)
    load_data.override(task_id="load_vacancies")(json_vacancies, sql_vacancies)
    load_data.override(task_id="load_applicants_logs")(applicant_logs, sql_app_logs)
    load_data.override(task_id="load_vacancies_logs")(vacancy_logs, sql_vac_logs)


huntflow_load()
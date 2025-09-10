class OzonApi(BaseApiClient):
    def __init__(
        self, client_id: str, token: str, logger: Optional[Callable[[str], None]] = None
    ):
        """
        Инициализация клиента OZON API.
        :param client_id: идентификатор клиента в OZON API
        :param token: API-ключ для доступа
        :param logger: опциональный логгер (callable), если None — используется стандартный logging
        """
        headers = {"Client-Id": client_id, "Api-Key": token}
        self.headers = headers
        super().__init__(
            base_url="https://api-seller.ozon.ru",
            token=token,
            headers=headers,
            logger=logger,
        )

    def fetch_products(self, project_name: str) -> pd.DataFrame:
        """
        Получает справочник товаров из OZON через API /v4/product/info/attributes.
        :param project_name: название проекта (для проставления в колонку project)
        :return: DataFrame со списком товаров (mp_id, barcode, mp_sku, product_name, project)
        """
        body = {
            "filter": {"visibility": "ALL"},
            "last_id": "",
            "limit": 100,
        }
        products = []

        while True:
            response = self.post(endpoint="/v4/product/info/attributes", json=body)

            if response and response["result"]:
                products.extend(response["result"])
                body["last_id"] = response["last_id"]

            if len(response["result"]) < body["limit"]:
                break

        output_df = pd.DataFrame(products)
        output_df = output_df[["sku", "barcode", "offer_id", "name"]]
        output_df["sku"] = output_df["sku"].astype(str)
        output_df["barcode"] = output_df["barcode"].astype(str)
        output_df["offer_id"] = output_df["offer_id"].astype(str)
        output_df["project"] = project_name

        output_df = output_df.rename(
            columns={"sku": "mp_id", "offer_id": "mp_sku", "name": "product_name"}
        )
        return output_df

    def _prepare_report(
        self,
        date_offset: int,
        date_period: int,
        delivery_schema: OzonDeliverySchema = OzonDeliverySchema.FBO
    ) -> Dict[str, Any]:
        """
        Создаёт отчёт в OZON по продажам/заказам через API /v1/report/postings/create.
        :param date_offset: сдвиг по датам (в днях) от текущего дня
        :param date_period: период (в днях), за который нужно построить отчёт
        :param delivery_schema: схема доставки (FBO или FBS)
        :return: словарь с кодом созданного отчёта (report_code)
        """
        _today = datetime.datetime.now(tz=timezone("Europe/Moscow")).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start_date = _today - datetime.timedelta(date_offset + date_period)
        end_date = _today - datetime.timedelta(date_offset - 1)

        body = {
            "filter": {
                "processed_at_from": start_date.isoformat(),
                "processed_at_to": end_date.isoformat(),
                "delivery_schema": [delivery_schema.value],
            },
            "language": "en",
        }

        response = self.post("/v1/report/postings/create", json=body)
        logger.info(response)
        return response["result"]

    def _check_report_status(
        self, code: Dict[str, Any], retries: int = 5, timeout: int = 60
    ) -> (bool, Optional[str]):
        """
        Проверяет статус готовности отчёта по коду.
        :param code: код отчёта (из _prepare_report)
        :param retries: число попыток проверки
        :param timeout: задержка между проверками (секунды)
        :return: (is_ready, file_link) — готовность и ссылка на файл, если готов
        """
        is_ready = False
        file_link = None

        for _ in range(retries):
            response = self.post("/v1/report/info", json=code)
            logger.info(response)
            if response and response["result"]:
                report_status = response["result"].get("status")
                if report_status == "success":
                    is_ready = True
                    file_link = response["result"].get("file")
                    break
            time.sleep(timeout)

        return is_ready, file_link

    def _fetch_ready_report(self, file_link: str) -> pd.DataFrame:
        """
        Скачивает готовый отчёт по ссылке file_link.
        :param file_link: URL на CSV-отчёт
        :return: DataFrame с содержимым отчёта
        """
        response = self.request_url(method=ApiMethod.GET, full_url=file_link)
        return pd.read_csv(BytesIO(response.content), sep=";")

    def fetch_orders_sales_report(self, date_offset: int, date_period: int) -> pd.DataFrame:
        """
        Получает отчёт по продажам/заказам из OZON для схем FBO и FBS.
        :param date_offset: сдвиг по датам (в днях) от текущего дня
        :param date_period: период (в днях), за который нужно построить отчёт
        :return: DataFrame с объединённым отчётом по FBO и FBS
        :raises ValueError: если отчёт не готов в течение заданного времени
        """
        output_dfs = []
        for schema in [OzonDeliverySchema.FBO, OzonDeliverySchema.FBS]:
            created_report_code = self._prepare_report(
                date_offset=date_offset,
                date_period=date_period,
                delivery_schema=schema
            )

            status, file_link = self._check_report_status(code=created_report_code)
            if not status or not file_link:
                raise ValueError("Report didn't generated for 5 minutes")

            output_dfs.append(self._fetch_ready_report(file_link))

        return pd.concat(output_dfs, ignore_index=True)

    def fetch_stocks_report(self, sku_list: List[str]) -> pd.DataFrame:
        """
        Получает аналитику по остаткам товаров с OZON через API /v1/analytics/stocks.
        :param sku_list: список артикулов (sku) для запроса
        :return: DataFrame с остатками: sku, amount, mp
        :raises ValueError: если список sku пуст или ответ API некорректен
        """
        def chunked(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i: i + n]

        sku_list = [str(s) for s in sku_list if str(s).isdigit() and int(s) > 0]
        if not sku_list:
            raise ValueError("Список SKU пуст или некорректен")

        all_results = []
        for batch in chunked(sku_list, 100):
            payload = {"skus": batch}
            response = self.post("/v1/analytics/stocks", json=payload)

            if not response or "items" not in response:
                self.logger(f"OZON API error or empty response: {response}")
                continue
            all_results.extend(response["items"])

        if not all_results:
            self.logger("Ответ OZON API пуст — возможно, нет остатков")
            return pd.DataFrame(columns=["sku", "amount", "mp"])

        df = pd.DataFrame(all_results)
        if "available_stock_count" not in df.columns or "offer_id" not in df.columns:
            raise ValueError(
                f"Некорректный ответ OZON API. Колонки: {df.columns.tolist()}"
            )

        df = df[["offer_id", "available_stock_count"]].rename(
            columns={"offer_id": "sku", "available_stock_count": "amount"}
        )
        df = df.groupby("sku", as_index=False)["amount"].sum()
        df["mp"] = "OZ"

        return df
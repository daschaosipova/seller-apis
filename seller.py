import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получить список товаров магазина озон
    
    Args:
        last_id (str): Идентификатор последнего товара из предыдущего запроса.
            Для первого запроса передается пустая строка ("").
        client_id (str): Идентификатор клиента в системе Ozon.
            Предоставляется при регистрации приложения.
        seller_token (str): API-ключ продавца для авторизации запросов.
            Выдается в личном кабинете Ozon.
    Returns:
        dict: Словарь с данными о товарах, содержащий ключи:
            - "items" (list): Список товаров в текущей порции.
            - "total" (int): Общее количество товаров, соответствующих фильтру.
            - "last_id" (str): Идентификатор последнего товара для следующего запроса.
    Raises:
        requests.exceptions.HTTPError: Если API вернул код ошибки (4xx, 5xx).
        KeyError: Если структура ответа API не соответствует ожидаемой.
    Example:
        >>> # Получение первой порции товаров
        >>> result = get_product_list("", "12345", "token123")
        >>> items = result.get("items", [])
        >>> last_id = result.get("last_id")
        >>> 
        >>> # Получение следующей порции
        >>> next_result = get_product_list(last_id, "12345", "token123")
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получить артикулы товаров магазина озон
        
    Args:
        client_id (str): Идентификатор клиента в системе Ozon.
            Выдается при регистрации приложения в личном кабинете.
        seller_token (str): API-ключ продавца для авторизации запросов.
            Предоставляется в разделе API настроек магазина.
    Returns:
        list: Список строк, содержащих артикулы (offer_id) всех товаров магазина.
            Пример: ["123456", "789012", "345678"]
    Raises:
        requests.exceptions.HTTPError: Если API Ozon вернул ошибку авторизации
            или другой код ошибки (4xx, 5xx).
        KeyError: Если структура ответа API изменилась и не содержит ожидаемых ключей.
    Example:
        >>> # Получение всех артикулов для последующего обновления
        >>> offer_ids = get_offer_ids("12345", "token123")
        >>> print(f"Найдено товаров: {len(offer_ids)}")
        >>> print(f"Первые 5 артикулов: {offer_ids[:5]}")
        >>> 
        >>> # Использование артикулов для обновления остатков
        >>> stocks = [{"offer_id": oid, "stock": 10} for oid in offer_ids]
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновить цены товаров
    
    Функция отправляет список с новыми ценами в API Ozon для массового обновления.
    За один запрос можно обновить цены до 1000 товаров. Цены должны быть указаны
    в рублях (RUB) без дробной части и форматирования.

    Args:
        prices (list): Список словарей с данными о ценах товаров.
        client_id (str): Идентификатор клиента в системе Ozon.
            Выдается при регистрации приложения.
        seller_token (str): API-ключ продавца для авторизации запросов.
            Предоставляется в личном кабинете Ozon.
    Returns:
        dict: Ответ API Ozon, содержащий результат обновления цен.
            Обычная структура ответа:
            {
                "result": list,     # Список обработанных товаров
                "errors": list      # Список ошибок валидации (если есть)
            }
    Raises:
        requests.exceptions.HTTPError: Если API вернул ошибку (4xx, 5xx).
        ValueError: Если prices не является списком или содержит некорректные данные.
    Example:
        >>> # Подготовка данных для обновления цен
        >>> prices_to_update = [
                {
                    "offer_id": "123456",
                    "price": "5990",
                    "old_price": "6990",
                 "currency_code": "RUB",
                    "auto_action_enabled": "UNKNOWN"
                },
                {
                    "offer_id": "789012",
                    "price": "12500",
                    "old_price": "0",
                    "currency_code": "RUB",
                    "auto_action_enabled": "UNKNOWN"
                }
            ]
        >>> 
        >>> # Отправка обновлений
        >>> result = update_price(prices_to_update, "client123", "token456")
        >>> print(f"Обновлено товаров: {len(result.get('result', []))}")
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновить остатки
    
    Функция отправляет информацию о количестве товаров в наличии в API Ozon.
    Остатки обновляются асинхронно и отображаются на витрине магазина в течение
    нескольких минут. За один запрос можно обновить остатки до 100 товаров.

    Args:
        stocks (list): Список словарей с данными об остатках товаров.
        client_id (str): Идентификатор клиента в системе Ozon.
            Выдается при регистрации приложения в личном кабинете.
        seller_token (str): API-ключ продавца для авторизации запросов.
            Предоставляется в разделе API настроек магазина.
    Returns:
        dict: Ответ API Ozon с результатом обработки запроса.
    Raises:
        requests.exceptions.HTTPError: Если API вернул ошибку HTTP.
        ValueError: Если stocks не является списком или содержит некорректные данные.
    Example:
        >>> # Обновление остатков нескольких товаров
        >>> stocks_to_update = [
                {"offer_id": "123456", "stock": 15},
                {"offer_id": "789012", "stock": 0},
                {"offer_id": "345678", "stock": 42}
            ]
        >>> result = update_stocks(stocks_to_update, "client123", "token456")
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачать файл ostatki с сайта casio
    
    Скачивает и обрабатывает файл с остатками товаров с внешнего источника.
    Функция выполняет полный цикл загрузки и подготовки данных об остатках.

    Returns:
        list[dict]: Список словарей, где каждый словарь представляет одну позицию товара.
            Каждый словарь содержит ключи, соответствующие колонкам Excel-файла.
    Raises:
        requests.exceptions.HTTPError: Если файл недоступен по указанному URL
            (404 - не найден, 403 - доступ запрещен, 500 - ошибка сервера).
        zipfile.BadZipFile: Если скачанный файл не является валидным ZIP-архивом
            (поврежден, неверный формат, пустой файл).
    Examples:
        >>> remainders = download_stock()
        >>> print(f"Загружено товаров: {len(remainders)}")
        Загружено товаров: 245
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Создает список остатков для обновления в Ozon на основе данных поставщика.

    Функция сопоставляет данные об остатках из файла поставщика со списком
    артикулов товаров в магазине Ozon и формирует структурированный список
    для последующей отправки в API. Товары, отсутствующие в файле поставщика,
    получают нулевой остаток. Во входном списке offer_ids функция удаляет
    обработанные артикулы.

    Args:
        watch_remnants (list[dict]): Список товаров из файла поставщика,
            полученный от функции download_stock(). Каждый словарь должен содержать ключи:
            - "Код" (str/int): Артикул товара в системе поставщика
            - "Количество" (str/int): Остаток товара (возможные значения: ">10", "1", число)
        
        offer_ids (list): Список артикулов товаров, загруженных в магазин Ozon.
            Обычно получается из функции get_offer_ids(). Содержит строковые
            значения артикулов.
    Returns:
        list[dict]: Список словарей с остатками для отправки в API Ozon.
    Examples:
        >>> create_stocks([{'Код': '001', 'Количество': '10'}], ['001'])
        [{'offer_id': '001', 'stock': 10}]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен для обновления в Ozon на основе данных поставщика.

    Функция формирует структурированный список с ценами для товаров, которые
    присутствуют как в файле поставщика, так и в магазине Ozon. 

    Args:
        watch_remnants (list[dict]): Список товаров из файла поставщика,
            полученный от функции download_stock().
        offer_ids (list): Список артикулов товаров, загруженных в магазин Ozon.
            Обычно получается из функции get_offer_ids(). Содержит строковые
            значения артикулов.
    Returns:
        list[dict]: Список словарей с ценами для отправки в API Ozon.
    Examples:
        >>> create_prices([{'Код': '001', 'Цена': "5'990.00 руб."}], ['001'])
        [{'offer_id': '001', 'price': 5990, 'currency_code': 'RUB'}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразует цену из формата поставщика в числовой вид.

    Функция удаляет все символы, кроме цифр, и отбрасывает дробную часть.
    Используется для подготовки цен перед отправкой в API маркетплейсов.

    Args:
        price (str): Цена в формате поставщика. Пример: "5'990.00 руб."
    Returns:
        str: Цена в виде строки, содержащей только цифры. Пример: "5990"
    Raises:
        AttributeError: Если аргумент price не является строкой.
    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'
        >>> price_conversion("12 500.50 ₽")
        '12500'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделить список lst на части по n элементов

    Функция-генератор, которая последовательно возвращает фрагменты исходного списка
    заданной длины. Используется для разбиения больших списков на порции,
    соответствующие ограничениям API маркетплейсов (например, 100 товаров
    для обновления остатков или 1000 для обновления цен).

    Args:
        lst (list): Исходный список, который необходимо разделить на части.
            Может содержать элементы любого типа.
        n (int): Размер одной части (количество элементов в подсписке).
            Должен быть положительным целым числом (n > 0).
    Yields:
        list: Следующая часть исходного списка, содержащая не более n элементов.
            Последняя часть может быть меньше n, если длина списка не кратна n.
    Raises:
        TypeError: Если lst не является списком или n не является целым числом.
        ValueError: Если n <= 0.
    Examples:
        >>> # Разделение списка чисел на части по 3
        >>> numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        >>> for chunk in divide(numbers, 3):
        ...     print(chunk)
        [1, 2, 3]
        [4, 5, 6]
        [7, 8, 9]
        [10]
        
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Загружает и обновляет цены товаров в магазине Ozon.

    Асинхронная функция, которая автоматизирует полный процесс обновления цен:
    1. Получает список всех артикулов товаров из магазина Ozon
    2. Создает структурированный список цен на основе данных поставщика
    3. Разбивает список на порции по 1000 товаров (ограничение API Ozon)
    4. Отправляет порции в API Ozon для обновления цен
    5. Возвращает сформированный список цен

    Args:
        watch_remnants (list[dict]): Список товаров из файла поставщика,
            полученный от функции download_stock().
        client_id (str): Идентификатор клиента в системе Ozon.
            Выдается при регистрации приложения в личном кабинете.
        seller_token (str): API-ключ продавца для авторизации запросов.
            Предоставляется в разделе API настроек магазина.
    Returns:
        list[dict]: Список словарей с ценами, которые были отправлены в API Ozon.
    Raises:
        requests.exceptions.HTTPError: Если API Ozon вернул ошибку при обновлении цен
            (неверный формат, проблемы авторизации, превышение лимитов).
        KeyError: Если в watch_remnants отсутствуют обязательные ключи ("Код", "Цена").
        ValueError: Если prices не является списком или содержит некорректные данные.
    Examples:
        >>> await upload_prices([{'Код': '001', 'Цена': "5'990.00 руб."}], '123', 'token123')
        [{'offer_id': '001', 'price': 5990}]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Загружает и обновляет остатки товаров в магазине Ozon.

    Асинхронная функция, которая автоматизирует полный процесс обновления остатков:
    1. Получает список всех артикулов товаров из магазина Ozon
    2. Создает структурированный список остатков на основе данных поставщика
       (с преобразованием по правилам: ">10" → 100, "1" → 0)
    3. Разбивает список на порции по 100 товаров (ограничение API Ozon)
    4. Отправляет порции в API Ozon для обновления остатков
    5. Возвращает два списка: товары в наличии и все обновленные остатки

    Args:
        watch_remnants (list[dict]): Список товаров из файла поставщика,
            полученный от функции download_stock().
        client_id (str): Идентификатор клиента в системе Ozon.
            Выдается при регистрации приложения в личном кабинете.
        seller_token (str): API-ключ продавца для авторизации запросов.
            Предоставляется в разделе API настроек магазина.
    Returns:
        tuple: Кортеж из двух элементов:
            - not_empty (list[dict]): Список товаров с ненулевым остатком (stock > 0)
            - stocks (list[dict]): Полный список всех обновленных остатков
    Raises:
        requests.exceptions.HTTPError: Если API Ozon вернул ошибку при обновлении остатков
            (неверный формат, проблемы авторизации, превышение лимитов).
        KeyError: Если в watch_remnants отсутствуют обязательные ключи ("Код", "Количество").
        ValueError: Если stocks не является списком или содержит некорректные данные.
    Examples:
        >>> await upload_stocks([{'Код': '001', 'Количество': '10'}], '123', 'token123')
        ([{'offer_id': '001', 'stock': 10}], [{'offer_id': '001', 'stock': 10}])
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()

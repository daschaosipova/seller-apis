import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров из кампании Яндекс Маркета с пагинацией.

    Функция отправляет запрос к API Яндекс Маркета и возвращает порцию товаров,
    связанных с указанной кампанией. Используется для последовательного получения
    всех товаров магазина с поддержкой пагинации через page_token.

    Args:
        page (str): Токен страницы для пагинации.
        campaign_id (str): Идентификатор рекламной кампании в Яндекс Маркете.
            Выдается при регистрации магазина и настройке кампании.
        access_token (str): Токен доступа для авторизации в API Яндекс Маркета.
            Получается в личном кабинете продавца в разделе API.
    
    Returns:
        dict: Словарь с данными о товарах кампании, содержащий ключи:
            - "offerMappingEntries" (list): Список товаров в текущей порции
            - "paging" (dict): Информация о пагинации с ключом "nextPageToken"
    
    Raises:
        requests.exceptions.HTTPError: Если API вернул ошибку HTTP.
        KeyError: Если структура ответа API не содержит ожидаемого ключа "result".
    
    Examples:
        >>> get_product_list("page123", "campaign123", "access_token123")
        [{'item': 'product123', 'price': 100.0},
        {'item': 'product124', 'price': 150.0}]
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет остатки товаров на складах в Яндекс Маркете.

    Функция отправляет PUT-запрос к API Яндекс Маркета для массового обновления
    информации о количестве товаров на складах кампании. Поддерживает обновление
    остатков как для FBS, так и для DBS схем работы.

    Args:
        campaign_id (str): Идентификатор рекламной кампании в Яндекс Маркете.
            Выдается при регистрации магазина и настройке кампании.
        stocks (list[dict]): Список словарей с информацией об остатках товаров.        
        access_token (str): Токен доступа для авторизации в API Яндекс Маркета.
            Получается в личном кабинете продавца в разделе API.
            Используется в заголовке Authorization: Bearer {token}.

    
    Returns:
        dict: Ответ API Яндекс Маркета о результате обновления остатков.
    
    Raises:
        requests.exceptions.HTTPError: Если API вернул ошибку HTTP.
        ValueError: Если stocks не является списком или содержит некорректные данные.
    
    Examples:
        >>> update_stocks([{"stock": 5, "item_id": "123"}],
        >>>               "campaign123", "access_token123")
        {'result': 'success'}

        >>> update_stocks("5", "campaign123", "access_token123")
        TypeError: expected list
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров в кампании Яндекс Маркета.

    Функция отправляет POST-запрос к API Яндекс Маркета для массового обновления
    цен на товары. Поддерживает установку новой цены, старой цены (для отображения
    скидки) и различных параметров ценообразования.

    Args:
        prices (list[dict]): Список словарей с информацией о ценах товаров.        
        campaign_id (str): Идентификатор рекламной кампании в Яндекс Маркете.
            Выдается при регистрации магазина и настройке кампании.
        access_token (str): Токен доступа для авторизации в API Яндекс Маркета.
            Получается в личном кабинете продавца в разделе API.
            Используется в заголовке Authorization: Bearer {token}.

    Returns:
        dict: Ответ API Яндекс Маркета о результате обновления цен.

    Raises:
        requests.exceptions.HTTPError: Если API вернул ошибку HTTP.
        ValueError: Если prices не является списком или содержит некорректные данные.
    
    Examples:
        >>> update_price([{"price": "5990", "item_id": "123"}],
        >>>              "campaign123", "access_token123")
        {'result': 'success'}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получает артикулы товаров Яндекс маркета
    
    Функция автоматически обрабатывает пагинацию и последовательно загружает
    все товары кампании, после чего извлекает из них артикулы (shopSku).
    Артикулы используются для дальнейшей синхронизации цен и остатков.

    Args:
        campaign_id (str): Идентификатор рекламной кампании в Яндекс Маркете.
            Выдается при регистрации магазина и настройке кампании.
        market_token (str): Токен доступа для авторизации в API Яндекс Маркета.
            Получается в личном кабинете продавца в разделе API.
            Используется в заголовке Authorization: Bearer {token}.

    Returns:
        list: Список строк, содержащих артикулы (shopSku) всех товаров кампании.

    Raises:
        requests.exceptions.HTTPError: Если API Яндекс Маркета вернул ошибку
            (неверный токен, кампания не найдена, проблемы с пагинацией).
        AttributeError: Если структура ответа API не содержит ожидаемых ключей
            (paging, nextPageToken, offer, shopSku).
        KeyError: При отсутствии обязательных полей в ответе API.
    
    Examples:
        >>> get_offer_ids("campaign123", "market_token123")
        ['offer123', 'offer124']

        >>> get_offer_ids("campaign123", 123)
        TypeError: expected string or bytes-like object
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создает список остатков для обновления в Яндекс Маркете на основе данных поставщика.

    Функция сопоставляет данные об остатках из файла поставщика со списком
    артикулов товаров в кампании Яндекс Маркета и формирует структурированный список
    для последующей отправки в API. Товары, отсутствующие в файле поставщика,
    получают нулевой остаток. Во входном списке offer_ids функция удаляет
    обработанные артикулы.

    Args:
        watch_remnants (list[dict]): Список товаров из файла поставщика,
            полученный от функции download_stock(). 
        offer_ids (list): Список артикулов товаров, загруженных в кампанию Яндекс Маркета.
            Обычно получается из функции get_offer_ids(). Содержит строковые
            значения артикулов (shopSku).
        warehouse_id (int): Идентификатор склада в Яндекс Маркете.
            Может быть ID склада FBS или DBS в зависимости от схемы работы.
            Получается в личном кабинете продавца.

    Returns:
        list[dict]: Список словарей с остатками для отправки в API Яндекс Маркета.
    
    Examples:
        >>> create_stocks([{"Код": "123", "Количество": ">10"}],
        >>>               ["123"], "warehouse123")
        [
            {'offer_id': '123', 'stock': 100}
        ]

        >>> create_stocks([{"Код": "123", "Количество": ">10"}],
        >>                "invalid", "warehouse123")
        TypeError: expected list
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен для обновления в Яндекс Маркете на основе данных поставщика.

    Функция формирует структурированный список с ценами для товаров, которые
    присутствуют как в файле поставщика, так и в кампании Яндекс Маркета.
    Цены проходят конвертацию из формата поставщика в числовой вид и преобразуются
    в формат, требуемый API Яндекс Маркета.

    Args:
        watch_remnants (list[dict]): Список товаров из файла поставщика,
            полученный от функции download_stock().
        offer_ids (list): Список артикулов товаров, загруженных в кампанию Яндекс Маркета.
            Обычно получается из функции get_offer_ids(). Содержит строковые
            значения артикулов (shopSku).

    Returns:
        list[dict]: Список словарей с ценами для отправки в API Яндекс Маркета.
    
    Examples:
        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], ["123"])
        [{
            'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
            'offer_id': '123', 'old_price': '0', 'price': '5990'
        }]

        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], "invalid")
        TypeError: expected list
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Загружает и обновляет цены товаров на Яндекс Маркет.

    Асинхронная функция, которая автоматизирует полный процесс обновления цен:
    1. Получает список всех артикулов товаров из кампании Яндекс Маркета
    2. Создает структурированный список цен на основе данных поставщика
    3. Разбивает список на порции по 500 товаров (ограничение API Яндекс Маркета)
    4. Отправляет порции в API Яндекс Маркета для обновления цен
    5. Возвращает сформированный список цен

    Args:
        watch_remnants (list[dict]): Список товаров из файла поставщика,
            полученный от функции download_stock().
        campaign_id (str): Идентификатор рекламной кампании в Яндекс Маркете.
            Выдается при регистрации магазина и настройке кампании.
        market_token (str): Токен доступа для авторизации в API Яндекс Маркета.
            Получается в личном кабинете продавца в разделе API.
            Используется в заголовке Authorization: Bearer {token}.

    Returns:
        list[dict]: Список словарей с ценами, которые были отправлены в API Яндекс Маркета.

    Raises:
        requests.exceptions.HTTPError: Если API Яндекс Маркета вернул ошибку
            (неверный формат, проблемы авторизации, превышение лимитов).
        KeyError: Если в watch_remnants отсутствуют обязательные ключи ("Код", "Цена").
    
    Examples:
        >>> await upload_prices([{'Код': '001', 'Цена': "5'990.00 руб."}], '123', 'access_token123')
        [{'id': '001', 'price': {'value': 5990, 'currencyId': 'RUR'}}]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Загружает и обновляет остатки товаров в кампании Яндекс Маркета.

    Асинхронная функция, которая автоматизирует полный процесс обновления остатков:
    1. Получает список всех артикулов товаров из кампании Яндекс Маркета
    2. Создает структурированный список остатков на основе данных поставщика
       (с преобразованием по правилам: ">10" → 100, "1" → 0)
    3. Разбивает список на порции по 2000 товаров (максимальный лимит API Яндекс Маркета)
    4. Отправляет порции в API Яндекс Маркета для обновления остатков
    5. Возвращает два списка: товары в наличии и все обновленные остатки

    Args:
        watch_remnants (list[dict]): Список товаров из файла поставщика,
            полученный от функции download_stock(). Каждый словарь должен содержать ключи:
            - "Код" (str/int): Артикул товара
            - "Количество" (str/int): Остаток товара (">10", "1" или число)
        campaign_id (str): Идентификатор рекламной кампании в Яндекс Маркете.
            Выдается при регистрации магазина и настройке кампании. 
        market_token (str): Токен доступа для авторизации в API Яндекс Маркета.
            Получается в личном кабинете продавца в разделе API.
            Используется в заголовке Authorization: Bearer {token}.
        warehouse_id (int): Идентификатор склада в Яндекс Маркете.
            Может быть ID склада FBS или DBS в зависимости от схемы работы.
            Получается в личном кабинете продавца.

    Returns:
        tuple: Кортеж из двух элементов:
            - not_empty (list[dict]): Список товаров с ненулевым остатком (count > 0)
            - stocks (list[dict]): Полный список всех обновленных остатков
    
    Examples:
        >>> await upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id)
        ([{"sku": "12345", "items": [{"count": 10}]}], [{"sku": "12345", "items": [{"count": 10}]}])

        >>> await upload_stocks(watch_remnants, campaign_id, market_token, wrong_warehouse_id)
        {"status":"OK","errors":[{"code":"str","message":"str"}]}
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """
    Основная функция для скачивания остатков, формирования данных о ценах и остатках и обновления информации на Яндекс Маркете для FBS и DBS кампаний.

    Функция выполняет следующие шаги:
    1. Загружает переменные окружения MARKET_TOKEN, FBS_ID, DBS_ID, WAREHOUSE_FBS_ID и WAREHOUSE_DBS_ID.
    2. Скачивает данные об остатках товаров.
    3. Получает список offer_id товаров для FBS кампании.
    4. Формирует данные об остатках и отправляет их в API Яндекс Маркета для FBS кампании.
    5. Формирует данные о ценах и отправляет их в API Яндекс Маркета для FBS кампании.
    6. Повторяет шаги 3-5 для DBS кампании.

    Обрабатывает исключения, возникающие при работе с сетью (requests) и другие общие исключения.
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()

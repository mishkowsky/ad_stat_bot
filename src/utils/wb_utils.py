import re
import urllib.parse
from dataclasses import dataclass
import requests

wb_sku_pattern = re.compile(r'\d{5,}')
wb_size_pattern = re.compile(r'(?<=size=)\d+')
wb_link_pattern = re.compile(r'(?:(?:(?:wb)|(?:wildberries))\.ru(?:(?:/catalog/)|(?:/product\?card=)))\d+')
non_wb_links = ('https://tgstat.ru/', 'https://ttttt.me/', 'https://t.me/', 'https://market.yandex.ru/')


def get_sku_from_url(link) -> int:
    link = urllib.parse.quote(link)
    return get_sku_from_text(link)


def get_sku_from_text(text: str) -> int:
    link_res = wb_link_pattern.findall(text)
    if link_res:
        sku = wb_sku_pattern.findall(link_res[0])
        if sku:
            return int(sku[0])


@dataclass(frozen=True)
class BrandRec:
    brand_id: int
    name: str


def get_brands_by_skus(skus: list[int]) -> dict[int, BrandRec]:
    x_info = 'appType=1&curr=rub&dest=-1257786&regions=68,64,83,4,38,80,33,70,82,86,75,30,69,1,48,22,66,31,40,71&spp=33'
    products = get_products(skus, x_info)
    result = dict()
    for product in products:
        result[product.get("id")] = BrandRec(product.get('brandId', ''), product.get('brand', ''))
    return result


def get_products(skus: list[int], x_info: str) -> list[dict]:
    multiplier = 0
    products = []
    while True:
        url = "https://card.wb.ru/cards/detail?" + x_info + "&nm="
        first = True
        to_add = skus[50 * multiplier: 50 * (multiplier + 1)]
        if not to_add:
            break
        multiplier += 1
        for vc in to_add:
            vc = str(vc)
            if first:
                url += str(vc)
                first = False
                continue
            url += f';{vc}'
        products.extend(requests.get(url).json().get("data").get("products"))
    return products

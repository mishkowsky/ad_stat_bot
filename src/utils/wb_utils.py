import re
import urllib.parse

wb_sku_pattern = re.compile(r'\d{5,}')
wb_size_pattern = re.compile(r'(?<=size=)\d+')
wb_link_pattern = re.compile(r'(?:(?:(?:wb)|(?:wildberries))\.ru(?:(?:/catalog/)|(?:/product\?card=)))\d+')


def get_sku_from_url(link):
    link = urllib.parse.quote(link)
    return get_sku_from_text(link)


def get_sku_from_text(text):
    link_res = wb_link_pattern.findall(text)
    if link_res:
        sku = wb_sku_pattern.findall(link_res[0])
        if sku:
            return int(sku[0])

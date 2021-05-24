# -*- coding: utf-8 -*-
import traceback
import re
import csv
import json
import time
import scrapy
import requests
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess
from uszipcode import SearchEngine

#PROXY = '37.48.118.90:13042'
PROXY = '208.110.64.202:3128'


def get_proxies_from_free_proxy():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.content)
    proxies = set()
    for i in parser.xpath('//tbody/tr'):
        if i.xpath('.//td[3][text()="US"]') and\
           i.xpath('.//td[7][contains(text(),"yes")]'):
            ip = i.xpath('.//td[1]/text()')[0]
            port = i.xpath('.//td[2]/text()')[0]
            proxies.add("{}:{}".format(ip, port))
            if len(proxies) == 20:
                return proxies
    return proxies


def get_states():
    return [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
        "Connecticut", "Delaware", "District of Columbia", "Florida",
        "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
        "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico",
        "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma",
        "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island",
        "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
        "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin",
        "Wyoming"
    ]


def get_zip_codes_map():
    search = SearchEngine()
    zipcodes = list()
    for state in get_states():
    # for state in ['New York']:
        final_response = list()
        response = search.by_state(state, returns=2000)
        for r in response:
            if r.major_city not in [x.major_city for x in final_response]:
                final_response.append(r)
        for res in response:
            if res:
                zipcodes.append({
                    'zip_code': res.zipcode,
                    'latitude': res.lat,
                    'longitude': res.lng,
                    'city': res.major_city,
                    'state': res.state
                })
    return sorted(zipcodes, key=lambda k: k['state'])


def get_vip_map():
    return {
        '1': 'VIP: DiamondPlus',
        '2': 'VIP: Diamond',
        '3': 'VIP: PlatinumPlus',
        '4': 'VIP: Platinum',
        '5': 'GoldPlus',
        '6': 'Gold',
        '8': 'Silver',
        '10': 'Bronze'
    }


def get_doc_type_map():
    return {
        'C': 'Orthodontist',
        'D': 'General Dentist'
    }


# def get_zip_codes_map():
#     search = SearchEngine()
#     zipcodes = list()
#     for state in get_states():
#     # for state in ['New York']:
#         response = search.by_state(state)
#         for res in response:
#             if res:
#                 # print(res)
#                 zipcodes.append({
#                     'zip_code': res.Zipcode,
#                     'latitude': res.Latitude,
#                     'longitude': res.Longitude,
#                     'city': res.City,
#                     'state': res.State
#                 })
#     return sorted(zipcodes, key=lambda k: k['state'])


class ExtractItem(scrapy.Item):
    AnnotationList = scrapy.Field()
    City = scrapy.Field()
    Country = scrapy.Field()
    Distance = scrapy.Field()
    DocID = scrapy.Field()
    DoctorType = scrapy.Field()
    Reputation = scrapy.Field()
    Fax = scrapy.Field()
    FirstName = scrapy.Field()
    FullName = scrapy.Field()
    HasAdditionalLocations = scrapy.Field()
    HasBio = scrapy.Field()
    HasEmail = scrapy.Field()
    IsCABMember = scrapy.Field()
    IsFacultyMember = scrapy.Field()
    IsItero = scrapy.Field()
    IsTeenDoctor = scrapy.Field()
    IsTeenGuarantee = scrapy.Field()
    IsVip = scrapy.Field()
    ThirdPartyFinancing = scrapy.Field()
    LastName = scrapy.Field()
    Latitude = scrapy.Field()
    Line1 = scrapy.Field()
    Line2 = scrapy.Field()
    Line3 = scrapy.Field()
    Line4 = scrapy.Field()
    Longitude = scrapy.Field()
    Num = scrapy.Field()
    OfficePhone = scrapy.Field()
    PostalCode = scrapy.Field()
    SegmentCode = scrapy.Field()
    State = scrapy.Field()
    Url = scrapy.Field()
    ProfilePhoto = scrapy.Field()
    Accuracy = scrapy.Field()


class InvisalignSpider(scrapy.Spider):
    name = "invisalign_spider"
    allowed_domains = ["invisalign.com"]
    scraped_data = list()
    fieldnames = [
        "AnnotationList", "City", "Country", "Distance", "DocID",
        "DoctorType", "Reputation", "Fax", "FirstName", "FullName",
        "HasAdditionalLocations", "HasBio", "HasEmail", "IsCABMember",
        "IsFacultyMember", "IsItero", "IsTeenDoctor", "IsTeenGuarantee",
        "IsVip", "ThirdPartyFinancing", "LastName", "Latitude", "Line1",
        "Line2", "Line3", "Line4", "Longitude", "Num", "OfficePhone",
        "PostalCode", "SegmentCode", "State", "Url", "ProfilePhoto", "Accuracy"
    ]

    def start_requests(self):
        base_url = "https://api.invisalign.com/svc/rd?"\
            "cl=US&"\
            "f=F1&"\
            "s=S1&"\
            "rd=7.5&"\
            "rdi=2.5&"\
            "it=us2&"\
            "pst=1&"\
            "av=nl2&"\
            "mtcc=1&"\
            "mrd=50&"
        zip_codes_map = get_zip_codes_map()
        self.vip_map = get_vip_map()
        self.doc_type_map = get_doc_type_map()
        headers = {
            "content-encoding": "gzip",
            "content-type": "text/html; charset=utf-8",
            "authority" : "api.invisalign.com",
            "scheme": "https",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding" : "gzip, deflate, br",
            "accept-language" : "en-US,en;q=0.9",
            "cache-control" : "max-age=0",
            "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36"
        }

        for index, zip_code_map in enumerate(zip_codes_map, 1):
            url = f"{base_url}pc={zip_code_map['zip_code']}&"\
                  f"lat={zip_code_map['latitude']}&"\
                  f"lng={zip_code_map['longitude']}"
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True,
                headers=headers
            )

    def parse(self, response):
        if not response.status == 200:
            return
        json_response = json.loads(response.text)
        if not json_response['responseStatus'] == 200:
            return

        results = json_response['responseData']['results']
        for result in results:
            if result.get('DocID') not in self.scraped_data:
                item = ExtractItem()
                dict_to_write = {
                    k: result[k] for k in self.fieldnames
                    if k not in ['Reputation']
                }
                if dict_to_write.get('DoctorType'):
                    dict_to_write['DoctorType'] =\
                        self.doc_type_map.get(
                            dict_to_write['DoctorType'],
                            dict_to_write['DoctorType'])
                seg_code = dict_to_write.pop(
                    'SegmentCode', None)
                if seg_code:
                    dict_to_write['Reputation'] =\
                        self.vip_map.get(
                            str(seg_code),
                            seg_code
                        )
                item.update(dict_to_write)
                self.scraped_data.append(result['DocID'])
                yield item


def run_spider(no_of_threads, request_delay):
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': get_proxies_from_free_proxy(),
        'ROTATING_PROXY_BAN_POLICY': 'pipelines.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess(settings)
    process.crawl(InvisalignSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 40
    request_delay = 0.02
    run_spider(no_of_threads, request_delay)

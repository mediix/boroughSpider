from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field
#
from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser
#
from datetime import date, datetime, timedelta

class kensingtonSpider(Spider):
    name = 'kensSpider'

    domain = 'http://rbkc.gov.uk'

    pipeline = 'Kensington'

    def create_dates(self, start, end, delta):
        curr = self.start
        while curr < self.end:
            yield curr
            curr += self.delta

    def create_item_class(self, class_name, field_list):
        fields = {}
        for field_name in field_list:
            fields[field_name] = Field()

        fields.update({'domain': Field()})
        fields.update({'borough': Field()})
        fields.update({'documents_url': Field()})
        return type(class_name, (DictItem,), {'fields':fields})

    def __init__(self):
        self.allowed_domains = ["rbkc.gov.uk"]
        self.base_url = ["http://www.rbkc.gov.uk/planning/scripts/weeklyform.asp",
                         "http://www.rbkc.gov.uk/planning/scripts/weeklyresults.asp"]

    def start_requests(self):
        Req = []
        Req.append(FormRequest(self.base_url[0], method="GET", callback = self.parse_date_result))
        return Req

    def parse_date_result(self, response):
        #inspect_response(response)
        weekly_dates = []
        # for date in response.xpath("//select[@id='WeekEndDate']/option/@value").extract():
        for result in self.create_dates(date.today() - timedelta(days = 365), date(2014, 11, 21), timedelta(days = 7)):
            weekly_dates.append(result.strftime("%d-%m-%Y"))

        for date in weekly_dates:
            yield FormRequest(self.base_url[1], method="POST",
                                formdata={ 'WeekEndDate':date, 'order':'Received Date' },
                                callback = self.parse_search_result)

    def parse_search_result(self, response):
        #inspect_response(response)
        Req = []
        for href in response.xpath("//table[@id='Table1']//td/a/@href").extract():
            Req.append(FormRequest(self.domain + href, method="GET", callback = self.parse_item_result))
        return Req

    def parse_item_result(self, response):
        #inspect_response(response)

        strat = (parse_html,)

        tab = extract(response.body, strategy=strat)
        table = list(prototypes.convert_table(tab.xpath("//table")))[:6]
        result = {}
        for d in table: result.update(d)
        result = { key.replace(' ', '_').replace(':', '').lower(): value[0] for key, value in result.items() }

        kensingtonItem = self.create_item_class('kensingtonItem', result.keys())

        item = kensingtonItem()

        for key, value in result.items():
          try:
            if value == '':
                item[key] = 'n/a'
            elif value.isdigit():
                item[key] = value
            else:
                item[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
          except:
            item[key] = value

        try:
            url = response.xpath("//a[@href='#tabs-planning-6']/@href").extract()
            item["documents_url"] = response.url + url[0]
        except:
            item["documents_url"] = 'n/a'

        item["borough"] = "Royal Borough of Kensington and Chelsea"
        item["domain"] = self.domain

        return item

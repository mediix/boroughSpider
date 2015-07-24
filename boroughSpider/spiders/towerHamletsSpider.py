from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field

from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser

from datetime import date, datetime, timedelta

class towerHamletsSpider(Spider):
  name = 'towerSpider'
  pipeline = 'TowerHamlets'
  domain = 'http://www.towerhamlets.gov.uk'
  base_url = ["http://planreg.towerhamlets.gov.uk"]
  start_urls = ["http://planreg.towerhamlets.gov.uk/WAM/monthlyDecisions.do?action=init"]

  def create_dates(self, start, end, delta):
    curr = start
    while curr < end:
      yield curr
      curr += delta

  def create_item_class(self, class_name, field_list):
    fields = {}
    for field_name in field_list:
      fields[field_name] = Field()

    fields.update({'domain': Field()})
    fields.update({'borough': Field()})
    fields.update({'documents_url': Field()})
    return type(class_name, (DictItem,), {'fields':fields})

  def parse(self, response):
    # inspect_response(response)
    months = []

    for result in self.create_dates(date(2013, 1, 1), date.today(), timedelta(days = 31)):
      months.append(result.strftime('%b %y'))

    # for month in months:
    return [FormRequest.from_response(response,
                        formname = 'searchCaseFileTypeForm',
                        formdata = { 'areaCode':'%',
                                     'sortOrder':'3',
                                     'endDate':'0',
                                     'decisionType':'%' },
                        callback = self.parse_results)]

  def parse_results(self, response):
    # inspect_response(response, self)
    urls = response.xpath("//table[@id='searchresults']//tr//td//a/@href").extract()

    for url in urls:
      app_url = '{0}{1}'.format(self.base_url[0], str(url))
      yield FormRequest(app_url, method="GET", callback = self.parse_item)

  def parse_item(self, response):
    # inspect_response(response, self)
    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = list(prototypes.convert_table(tab.xpath("//table")))[0]
    table = { key.replace(':', '').replace(' ', '_').lower(): value[0].encode('utf-8') \
      if value else '' for key, value in table.items() }

    # import pdb; pdb.set_trace()

    towerHamletsItem = self.create_item_class('towerHamletsItem', table.keys())

    item = towerHamletsItem()

    for key, value in table.items():
      try:
        if value == '':
          item[key] = 'n/a'
        elif value.isdigit():
          item[key] = value
        else:
          item[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
      except:
        item[key] = value

    item['borough'] = "Tower Hamlets"
    item['domain'] = self.domain
    item['documents_url'] = response.url

    return item

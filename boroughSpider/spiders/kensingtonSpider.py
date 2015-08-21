from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
# from scrapy.item import DictItem, Field
#
from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser
#
from datetime import date, datetime, timedelta

class kensigtonSpider(Spider):
  name = 'kensSpider'
  domain = 'http://rbkc.gov.uk'
  pipeline = ['Kensington']
  base_url = ["http://www.rbkc.gov.uk/planning/scripts/weeklyform.asp",
              "http://www.rbkc.gov.uk/planning/scripts/weeklyresults.asp"]

  def create_dates(self, start, end, delta):
    curr = start
    while curr < end:
      yield curr
      curr += delta

  def start_requests(self):
    return [FormRequest(self.base_url[0], method="GET", callback = self.parse_date_result)]

  def parse_date_result(self, response):
    #inspect_response(response)
    weeks = []
    for res in self.create_dates(date(2010, 1, 1), date(date.today().year, (date.today().month+1)%12, 1), timedelta(days=7)):
      weeks.append(res.strftime("%d/%m/%Y"))

    for week in weeks[::-1]:
      yield FormRequest(self.base_url[1], method="POST",
                            formdata={ 'WeekEndDate':week, 'order':'Received Date' },
                            callback = self.parse_search_result)

  def parse_search_result(self, response):
    #inspect_response(response)
    for href in response.xpath("//table[@id='Table1']//td/a/@href").extract():
      yield FormRequest(self.domain + href, method="GET", callback = self.parse_item_result)

  def parse_item_result(self, response):
    #inspect_response(response)
    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = list(prototypes.convert_table(tab.xpath("//table")))[:6]
    result = {}
    for d in table: result.update(d)
    result = { k.replace(' ', '_').replace(':', '').lower(): v[0] for k, v in result.items() }

    for key, value in result.items():
      try:
        if value == '':
          result[key] = ''
        elif value.isdigit():
          result[key] = value
        else:
          result[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
      except:
        result[key] = value

    result.update({'borough': "Royal Borough of Kensington and Chelsea"})
    result.update({'domain': self.domain})

    try:
        url = response.xpath("//a[@href='#tabs-planning-6']/@href").extract()
    except Exception as err:
        result.update({'documents_url': 'n/a'})
    else:
        result.update({'documents_url':response.url + url[0].encode('utf-8')})

    return result

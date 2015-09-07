from scrapy.spiders import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field

from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser


class westminsterSpider(Spider):
  name = 'westSpider'
  pipeline = ['Westminster']
  domain = 'http://idoxpa.westminster.gov.uk'
  base_url = ["dummy", "http://idoxpa.westminster.gov.uk"]
  start_urls = ["http://idoxpa.westminster.gov.uk/online-applications/search.do?action=monthlyList"]

  custom_settings = {
      'DOWNLOAD_DELAY': 0.5,
      'RETRY_ENABLED': True,
      'CONCURRENT_REQUESTS': 1,
      'CONCURRNT_REQUESTS_PER_IP': 1,
      'RANDOM_DOWNLOAD_DELY': False,
      'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
      'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
  }

  def __init__(self, month, **kwargs):
    self.month = month

  def parse(self, response):
    # inspect_response(response)
    parishes = response.xpath("//*[@id='parish']/option/@value").extract()[1:]
    for parish in parishes:
      yield FormRequest.from_response(response,
                          formname = 'searchCriteriaForm',
                          formdata = { 'searchCriteria.parish':'WS',
                                       'month':self.month,
                                       'dateType':'DC_Validated',
                                       'searchType':'Application' },
                          callback = self.parse_results)

  def parse_results(self, response):
    # inspect_response(response)
    return [FormRequest.from_response(response,
                          formname = 'searchCriteriaForm',
                          formdata = { 'searchCriteria.page':'1',
                                       'action':'page',
                                       'orderBy':'DateReceived',
                                       'orderByDirection':'Descending',
                                       'searchCriteria.resultsPerPage':'100' },
                          callback = self.parse_long_resutls)]

  def parse_long_resutls(self, response):
    # inspect_response(response)

    if response.xpath("//*[@class='pager top']/span[@class='showing']"):
      for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
        item_url = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
        yield FormRequest(item_url, method="GET", callback=self.parse_summary)

      for href in response.xpath("//p[@class='pager top']/a[@class='page']/@href").extract():
        nxt_url = '{0}{1}'.format(self.base_url[1], href.encode('utf-8'))
        yield FormRequest(nxt_url, method="GET", callback=self.parse_items)

    else:
      for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
        item_url = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
        yield FormRequest(item_url, method="GET", callback=self.parse_summary)

  def parse_items(self, response):
    for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[1], str(url))
      yield FormRequest(item_url, method="GET", callback=self.parse_summary)

  def parse_summary(self, response):
    # inspect_response(response)
    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except IndexError as err:
      pass

    if response.xpath("//*[@id='subtab_details']/@href").extract():
      further_info_url = response.xpath("//*[@id='subtab_details']/@href").extract()[0]
      further_info_url = '{0}{1}'.format(self.base_url[1], further_info_url.encode('utf-8'))
      return [FormRequest(further_info_url, method = "GET",
                                            meta = {'table':table},
                                            callback = self.parse_further_info)]
    else:
      pass

  def parse_further_info(self, response):
    # inspect_response(response)
    table = response.meta['table']

    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table_1 = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except IndexError as err:
      pass
    else:
      table.update(table_1)

    if response.xpath("//*[@id='subtab_dates']").extract():
      url = response.xpath("//*[@id='subtab_dates']/@href").extract()[0]
      important_dates = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
      return FormRequest(important_dates, method="GET", meta={'table':table}, callback=self.parse_important_dates)

    else:
      table = { k.replace(' ', '_').lower(): v[0].encode('utf-8') for k, v in table.items() }

      for key, value in table.items():
        try:
          if value == '':
            table[key] = ''
          elif value.isdigit():
            table[key] = value
          else:
            table[key] = parser.parse(value.encode('utf-8')).strftime("%Y-%m-%d")
        except Exception as err:
          table[key] = value

      table.update({'borough': "City of Westminster"})
      table.update({'domain': self.domain})

      try:
        documents_url = response.xpath("//*[@id='tab_documents']/@href").extract()[0]
        documents_url = '{0}{1}'.format(self.base_url[1], documents_url.encode('utf-8'))
        table.update({'documents_url': documents_url})
      except Exception as err:
        table.update({'documents_url': 'n/a'})

      return item

  def parse_important_dates(self, response):
    # inspect_response(response, self)
    table = response.meta['table']

    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table_1 = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except IndexError as err:
      pass
    else:
      table.update(table_1)

    table = { k.replace(' ', '_').lower(): v[0].encode('utf-8') for k, v in table.items() }

    for key, value in table.items():
      try:
        if value == '':
          table[key] = ''
        elif value.isdigit():
          table[key] = value
        else:
          table[key] = parser.parse(value.encode('utf-8')).strftime("%Y-%m-%d")
      except Exception as err:
        table[key] = value

    table.update({'borough': "City of Westminster"})
    table.update({'domain': self.domain})

    try:
      documents_url = response.xpath("//*[@id='tab_documents']/@href").extract()[0]
    except Exception as err:
      table.update({'documents_url': 'n/a'})
    else:
      documents_url = '{0}{1}'.format(self.base_url[1], documents_url.encode('utf-8'))
      table.update({'documents_url': documents_url})

    return table

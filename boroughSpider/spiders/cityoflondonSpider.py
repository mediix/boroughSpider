from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field

from libextract import extract, prototypes
from libextract.tabular import parse_html
from datetime import date, datetime, timedelta
from dateutil import parser

class cityOfLondonSpider(Spider):
  name = 'londSpider'
  # pipeline = 'CityOfLondon'
  pipeline = 'GenericPipeline'
  domain = 'cityoflondon.gov.uk'
  base_url = ["http://www.planning2.cityoflondon.gov.uk/online-applications/pagedSearchResults.do?action=page&searchCriteria.page=", "http://www.planning2.cityoflondon.gov.uk"]
  start_urls = ["http://www.planning2.cityoflondon.gov.uk/online-applications/search.do?action=monthlyList"]

  def __init__(self, month=None):
    self.month = month
    self.start_urls = ["http://www.planning2.cityoflondon.gov.uk/online-applications/search.do?action=monthlyList"]

  def create_item_class(self, class_name, field_list):
    fields = {}
    for field_name in field_list:
      fields[field_name] = Field()

    fields.update({'domain': Field()})
    fields.update({'borough': Field()})
    fields.update({'documents_url': Field()})
    return type(class_name, (DictItem,), {'fields':fields})

  def parse(self, response):
    print "MONTH:", self.month
    # inspect_response(response)
    # url = response.xpath("//*[@name='searchCriteriaForm']/@action").extract()[0]
    # post_url = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
    # for month in response.xpath("//*[@id='month']/option/text()").extract():
    # for month in months:
    return [FormRequest.from_response(response,
                        formname = 'searchCriteriaForm',
                        formdata = { 'searchCriteria.caseStatus':'',
                                     'searchCriteria.ward':'',
                                     'month':self.month,
                                     'dateType':'DC_Validated',
                                     'searchType':'Application' },
                        callback = self.parse_results)]

  def parse_results(self, response):
    # inspect_response(response)
    # url = response.xpath("//*[@name='searchCriteriaForm']/@action").extract()[0]
    # post_url = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
    return [FormRequest.from_response(response,
                          formname = 'searchCriteriaForm',
                          formdata = { 'searchCriteria.page':'1',
                                        'action':'page',
                                        'orderBy':'DateReceived',
                                        'orderByDirection':'Descending',
                                        'searchCriteria.resultsPerPage':'100' },
                          callback = self.parse_long_results)]

  # def parse_results(self, response):
  #   # inspect_response(response, self)
  #   try:
  #     pages = response.xpath("//p[@class='pager bottom']/span[@class='showing'] \
  #               /text()[(preceding-sibling::strong)]").extract()[0].encode('utf-8')
  #     pages = int(pages.split()[1])
  #     pages = (pages/10) + (pages % 10 > 0)
  #     #
  #     for page_num in xrange(1, pages+1):
  #       url = '{0}{1}'.format(self.base_url[0], page_num)
  #       yield FormRequest(url, method="GET", callback = self.parse_items)
  #   except Exception as err:
  #     print "Error -> parse_results", err
  #     pass

  #   try:
  #     for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
  #       item_url = '{0}{1}'.format(self.base_url[1], url)
  #       yield FormRequest(item_url, method="GET", callback = self.parse_summary)
  #   except Exception as err:
  #     pass

  def parse_long_results(self, response):
    # inspect_response(response, self)

    # if there exists pagination
    if response.xpath("//*[@class='pager top']/span[@class='showing']"):
      # parse current page items
      for href in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
        item_url = '{0}{1}'.format(self.base_url[1], href.encode('utf-8'))
        yield FormRequest(item_url, method="GET", callback = self.parse_summary)
      # parse the next page of items
      for url in response.xpath("//p[@class='pager top']/a[@class='page']/@href").extract():
        nxt_url = '{0}{1}'.format(self.base_url[0], url.encode('utf-8'))
        yield FormRequest(nxt_url, method="GET", callback = self.parse_items)
    # if there is not pagination
    else:
      for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
        item_url = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
        yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_items(self, response):
    # inspect_response(response, self)
    for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
      yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_summary(self, response):
    # inspect_response(response)
    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except IndexError as err:
      pass

    if response.xpath("//*[@id='subtab_details']/@href").extract():
      url = response.xpath("//*[@id='subtab_details']/@href").extract()[0]
      further_info = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
      return FormRequest(further_info, method = "GET", meta={'table':table}, callback=self.parse_further_info)
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
      table = { key.replace(' ', '_').lower(): value[0].encode('utf-8') for key, value in table.items() }
      cityoflondonItem = self.create_item_class('cityoflondonItem', table.keys())
      item = cityoflondonItem()

      for key, value in table.items():
        try:
          if value == '':
            item[key] = 'n/a'
          elif value.isdigit():
            item[key] = value
          else:
            item[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
        except Exception as err:
          item[key] = value

      item['borough'] = "City of London"
      item['domain'] = self.domain
      try:
        documents_url = response.xpath("//*[@id='tab_documents']/@href").extract()[0]
        documents_url = '{0}{1}'.format(self.base_url[1], documents_url)
        item['documents_url'] = documents_url
      except Exception as err:
        item['documents_url'] = "n/a"

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

    table = { key.replace(' ', '_').lower(): value[0].encode('utf-8') for key, value in table.items() }
    cityoflondonItem = self.create_item_class('cityoflondonItem', table.keys())
    item = cityoflondonItem()

    for key, value in table.items():
      try:
        if value == '':
          item[key] = 'n/a'
        elif value.isdigit():
          item[key] = value
        else:
          item[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
      except Exception as err:
        item[key] = value

    item['borough'] = "City of London"
    item['domain'] = self.domain
    try:
      documents_url = response.xpath("//*[@id='tab_documents']/@href").extract()[0]
      documents_url = '{0}{1}'.format(self.base_url[1], documents_url)
      item['documents_url'] = documents_url
    except Exception as err:
      item['documents_url'] = "n/a"

    return item

from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field
#
from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser, relativedelta
#
from datetime import date, datetime, timedelta
import time


class SouthwarkSpider(Spider):
  name = 'soutSpider'
  domain = 'southwark.gov.uk'
  pipeline = 'Southwark'
  base_url = ["http://planbuild.southwark.gov.uk:8190/online-applications/pagedSearchResults.do?action=page&searchCriteria.page=",
              "http://planbuild.southwark.gov.uk:8190"]
  start_urls = ["http://planbuild.southwark.gov.uk:8190/online-applications/search.do?action=monthlyList"]

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
  	#
    months = []
    for result in self.create_dates(date(2013, 1, 1), date.today(), timedelta(days = 31)):
      months.append(result.strftime('%b %y'))

    for month in months[14:-13]:
      yield FormRequest.from_response(response,
    			formname = 'searchCriteriaForm',
    			formdata = { 'month':str(month),
    							 		 'dateType': 'DC_Validated',
    							 		 'searchType':'Application' },
    			callback = self.parse_results)

  def parse_results(self, response):
    # inspect_response(response)
    try:
      yield FormRequest.from_response(response,
                          formname = 'searchCriteriaForm',
                          formdata = { 'searchCriteria.page':'1',
                                       'action':'page',
                                       'orderBy':'DateReceived',
                                       'orderByDirection':'Descending',
                                       'searchCriteria.resultsPerPage':'100' },
                          callback = self.parse_long_results)
    except:
      pass

  def parse_long_results(self, response):
    # inspect_response(response, self)
    #
    if response.xpath("//*[@class='pager top']/span[@class='showing']"):
      for href in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
        item_url = '{0}{1}'.format(self.base_url[1], str(href))
        yield FormRequest(item_url, method="GET", callback = self.parse_summary)

      for url in response.xpath("//p[@class='pager top']/a[@class='page']/@href").extract():
        nxt_url = '{0}{1}'.format(self.base_url[0], str(url))
        yield FormRequest(nxt_url, method="GET", callback = self.parse_items)

    else:
      try:
        for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
          item_url = '{0}{1}'.format(self.base_url[1], str(url))
          yield FormRequest(item_url, method="GET", callback = self.parse_items)
      except:
        pass

  def parse_items(self, response):
    # inspect_response(response, self)
    #
    for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[1], str(url))
      yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_summary(self, response):
    # inspect_response(response, self)

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = list(prototypes.convert_table(tab.xpath("//table")))[0]

    further_info_url = response.xpath("//*[@id='subtab_details']/@href").extract()[0]
    further_info_url = '{0}{1}'.format(self.base_url[1], further_info_url)
    return [FormRequest(further_info_url, method = "GET",
                          meta = {'table':table},
                          callback = self.parse_further_info)]

  def parse_further_info(self, response):
    # inspect_response(response)

    table = response.meta['table']

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table.update(list(prototypes.convert_table(tab.xpath("//table")))[0])

    important_dates_url = response.xpath("//*[@id='subtab_dates']/@href").extract()[0]
    important_dates_url = '{0}{1}'.format(self.base_url[1], important_dates_url)
    request = FormRequest(important_dates_url, method = "GET",
                          meta = {'table':table},
                          callback = self.parse_important_dates)
    return request

  def parse_important_dates(self, response):
    #inspect_response(response)

    table = response.meta['table']

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table.update(list(prototypes.convert_table(tab.xpath("//table")))[0])

    table = {key.replace(' ', '_').lower(): value[0].encode('utf-8') for key, value in table.items()}

    southwarkItem = self.create_item_class('southwarkItem', table.keys())
    item = southwarkItem()

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
    item['borough'] = "Southwark"
    item['domain'] = self.domain
    try:
      documents_url = response.xpath("//*[@id='tab_documents']/@href").extract()[0]
      documents_url = '{0}{1}'.format(self.base_url[1], str(documents_url))
      item['documents_url'] = documents_url
    except:
      item['documents_url'] = "n/a"

    return item

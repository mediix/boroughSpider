from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from boroughSpider.items import londonCityItem

from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser

import time

today = time.strftime("%x %X")

class cityOfLondonSpider(Spider):
  name = 'londoncitySpider'

  pipeline = 'CityOfLondon'

  domain = 'cityoflondon.gov.uk'

  base_url = ["http://www.planning2.cityoflondon.gov.uk/online-applications/pagedSearchResults.do?action=page&searchCriteria.page=",
              "http://www.planning2.cityoflondon.gov.uk"]

  start_urls = ["http://www.planning2.cityoflondon.gov.uk/online-applications/search.do?action=monthlyList"]

  def parse(self, response):
    for month in response.xpath("//*[@id='month']/option/text()").extract():
      yield FormRequest.from_response(response,
                        formname =   'searchCriteriaForm',
                        formdata = { 'searchCriteria.caseStatus':'',
                                     'searchCriteria.ward':'',
                                     'month':str(month),
                                     'dateType':'DC_Validated',
                                     'searchType':'Application' },
                        callback = self.parse_results)

  def parse_results(self, response):
    # inspect_response(response)
    if response.xpath("//p[@class='pager top']/span[@class='showing']"):
      pages = response.xpath("//*[@id='searchResultsContainer']/p[@class='pager top']/a/@href").extract()
      for page in xrange(1, len(pages)+1):
        page_url = '{0}{1}'.format(self.base_url[0], page)
        yield FormRequest(page_url, method="GET", callback = self.parse_items)
    else:
      for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
        item_url = '{0}{1}'.format(self.base_url[1], url)
        yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_items(self, response):
    # inspect_response(response)
    for app in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[1], app)
      yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_summary(self, response):
    # inspect_response(response)
    item = londonCityItem()

    for key in item.fields.keys():
      item[key] = ''

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = list(prototypes.convert_table(tab.xpath("//table")))[0]
    table = {key.replace(' ', '_').lower(): value[0] for key, value in table.items()}

    for key, value in table.items():
      try:
        if (kay == key for kay in item.fields.keys()) and value != '':
          item[key] = parser.parse(str(value), fuzzy=True).strftime("%Y-%m-%d")
        else:
          item[key] = value
      except:
        pass

    further_info_url = response.xpath("//*[@id='subtab_details']/@href").extract()[0]
    further_info_url = '{0}{1}'.format(self.base_url[1], further_info_url)
    request = FormRequest(further_info_url, method = "GET",
                          meta = {'item':item},
                          callback = self.parse_further_info)
    return request

  def parse_further_info(self, response):
    # inspect_response(response)

    item = response.meta['item']

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = list(prototypes.convert_table(tab.xpath("//table")))[0]
    table = {key.replace(' ', '_').lower(): value[0] for key, value in table.items()}

    for key, value in table.items():
      try:
        if (kay == key for kay in item.fields.keys()) and value != '':
          item[key] = parser.parse(str(value), fuzzy=True).strftime("%Y-%m-%d")
        else:
          item[key] = value
      except:
        pass

    item['borough'] = "City of London"
    item['domain'] = self.domain
    try:
      documents_url = response.xpath("//*[@id='tab_documents']/@href").extract()[0]
      documents_url = '{0}{1}'.format(self.base_url[1], documents_url)
      item['documents_url'] = documents_url
    except:
      item['documents_url'] = "n/a"

    return item

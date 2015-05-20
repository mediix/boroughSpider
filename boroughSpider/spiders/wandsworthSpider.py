from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field

from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser

import time

today = time.strftime("%x %X")

class wandsworthSpider(Spider):
  name = 'wandsSpider'

  domain = 'http://ww3.wandsworth.gov.uk'

  pipeline = 'Wandsworthspider'

  base_url = ["http://ww3.wandsworth.gov.uk/Northgate/PlanningExplorer/Generic/"]

  start_urls = ["http://ww3.wandsworth.gov.uk/Northgate/PlanningExplorer/GeneralSearch.aspx"]

  def create_item_class(self, class_name, field_list):
    fields = {}
    for field_name in field_list:
      fields[field_name] = Field()

    fields.update({'domain': Field()})
    fields.update({'borough': Field()})
    fields.update({'constraints': Field()})
    fields.update({'documents_url': Field()})
    return type(class_name, (DictItem,), {'fields':fields})

  def parse(self, response):
    return [FormRequest.from_response(response,
                                      formname = 'Template',
                                      formdata = { 'cboSelectDateValue':'DATE_RECEIVED',
                                                   'rbGroup':'rbMonth',
                                                   'cboMonths':'5' },
                                      callback = self.parse_search_result)]

  def parse_search_result(self, response):
    # inspect_response(response)

    # for item_url in response.xpath("//td[@title='View Application Details']//a/@href").extract():
    #   built_item_url = '{0}{1}'.format(self.base_url[0], item_url)

    yield  FormRequest(response.url, method="GET", callback = self.parse_applications)

    try:
      nxt = response.xpath("//div[@class='align_center']//node()[following::span and not(@class='noborder')]/@href").extract()
      for url in response.xpath("//a[@class='results_page_number']/@href").extract():
        next_url = '{0}{1}'.format(self.base_url[0], url)
        yield FormRequest(next_url, method="GET", callback = self.parse_search_result)
    except:
      pass

  def parse_applications(self, response):
    inspect_response(response)



    # strat = (parse_html,)

    # tab = extract(response.content, strategy=strat)

    # table = tab.xpath("//div[@class='dataview']//ul//li")

    # table = [[str(text.strip().encode('utf-8')).strip() for text in elem.itertext()] for elem in table]
    # table = [[x for x in elem if x != ''] for elem in table]
    # table = { t[0]:t[1:] for t in table }
    # table = { key.replace(' ', '_').lower(): (value[0] if value else '') for key, value in table.items() }


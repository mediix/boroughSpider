from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
# from boroughSpider.items import idoxpaItem

from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser

import time

today = time.strftime("%x %X")

class wandsworthSpider(Spider):
  name = 'wandsworthSpider'

  domain = 'http://ww3.wandsworth.gov.uk'

  pipeline = 'Wandsworthspider'

  start_urls = ["http://ww3.wandsworth.gov.uk/Northgate/PlanningExplorer/GeneralSearch.aspx"]

  def parse(self, response):
    return [FormRequest.from_response(response,
                                      formname = 'Template',
                                      formdata = { 'cboSelectDateValue':'DATE_RECEIVED',
                                                   'rbGroup':'rbMonth',
                                                   'cboMonths':'5' },
                                      callback = self.parse_search_result)]

  def parse_search_result(self, response):
    # inspect_response(response)
    item = []
    nxt = response.xpath("//a[@class='noborder']/@href").extract()[0]
    while (nxt):
      item += response.xpath()

    strat = (parse_html,)
    tab = extract(r.content, strategy=strat)
    table = tab.xpath("//div[@class='dataview']//ul//li")

    table = [[text for text in elem.itertext() if len(text.replace(u'\xa0',u'').replace('\r','').replace('\n','').replace('\t','').replace(' ',''))] for elem in table]
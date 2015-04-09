from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request,FormRequest
from scrapy.exceptions import CloseSpider
from boroughSpider.items import idoxpaItem
from scrapy import log
import urllib, re, time

today = time.strftime("%x %X")

class idoxpaSpider(Spider):
  name = 'idoxpaSpider'

  pipeline = 'idoxpaPipeline'

  domain = 'https://www.westminster.gov.uk'

  base_url = ["http://idoxpa.westminster.gov.uk/online-applications/pagedSearchResults.do?action=page&searchCriteria.page="]

  start_urls = ["http://idoxpa.westminster.gov.uk/online-applications/search.do?action=monthlyList"]

  def parse(self, response):
    for parish in response.xpath("//*[@id='parish']/option/@value").extract():
      for month in response.xpath("//*[@id='month']/option/text()").extract():
        yield FormRequest.from_response(response,
                          formname = 'searchCriteriaForm',
                          formdata = { 'searchCriteria.parish':parish,
                                       'month':month,
                                       'dateType':'DC_Validated',
                                       'searchType':'Application' },
                          callback = self.parse_results)

  def parse_results(self, response):
    inspect_response(response)
    try:
      num_of_pages = response.xpath("//p[@class='pager bottom']/span[@class='showing'] \
                                    /text()[(preceding-sibling::strong)]").extract()[0]
      num_of_pages = int(num_of_pages.split()[1])
      num_of_pages = (num_of_pages/10) + (num_of_pages % 10 > 0)
      #
      for page_num in xrange(1, num_of_pages+1):
        page_url = '{0}{1}'.format(self.base_url[0], num_of_pages)
        yield FormRequest(page_url, method="GET", callback = self.parse_items)
    except:
      for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
        item_url = '{0}{1}'.format(self.base_url[1], url)
        yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_items(self, response):
    for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[1], url)
      yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_summary(self, response):
    inspect_response(response)

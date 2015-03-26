from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request,FormRequest
from scrapy.exceptions import CloseSpider
from scrapy.http.cookies import CookieJar
from boroughSpider.items import ApplicationItem
from scrapy import log
import urllib, re, time

today = time.strftime("%x %X")

class idoxpaSpider(Spider):
  name = 'idoxpaSpider'
  domain = 'https://www.westminster.gov.uk'

  base_url = ["http://idoxpa.westminster.gov.uk/online-applications/pagedSearchResults.do?action=page&searchCriteria.page"]

  start_urls = ["http://idoxpa.westminster.gov.uk/online-applications/search.do?action=monthlyList"]

  def parse(self, response):
    for parish in response.xpath("//*[@id='parish']/option/@value").extract():
      for month in response.xpath("//*[@id='month']/option/text()").extract():
        yield FormRequest.from_response(response,
                          formname = 'searchCriteriaForm',
                          formdata = { 'searchCriteria.parish':parish, 'month':month, 'dateType':'DC_Validated', 'searchType':'Application' },
                          callback = self.parse_results)

  def parse_results(self, response):
    inspect_response(response)
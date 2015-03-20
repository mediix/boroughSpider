from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request,FormRequest
from scrapy.exceptions import CloseSpider
from boroughSpider.items import ApplicationItem
from scrapy import log
import csv, urllib, re, time, sys

today = time.strftime("%x %X")

class ibhfSpider(Spider):
  name = 'ibhfSpider'
  domain = 'http://ibhf.gov.uk'

  item = []
  items = {}

  base_url = ["http://public-access.lbhf.gov.uk/online-applications/pagedSearchResults.do?action=page&searchCriteria.page=",
              "http://public-access.lbhf.gov.uk"]

  start_urls = ["http://public-access.lbhf.gov.uk/online-applications/search.do?action=monthlyList"]

  def parse(self, response):
    for month in response.xpath("//*[@id='month']/option/text()").extract():
      yield FormRequest.from_response(response,
                        formname = 'searchCriteriaForm',
                        formdata = { 'month':month, 'dateType': 'DC_Validated', 'searchType':'Application' },
                        callback = self.parse_results)

  def parse_results(self, response):
    # Takes care of pagination
    num_of_pages = response.xpath("//p[@class='pager bottom']/span[@class='showing'] \
                                    /text()[(preceding-sibling::strong)]").extract()[0]
    num_of_pages = int(num_of_pages.split()[1])
    num_of_pages = (num_of_pages/10) + (num_of_pages % 10 > 0)
    #
    for page_num in range(1, num_of_pages+1):
      page_url = '{0}{1}'.format(self.base_url[0], num_of_pages)
      yield FormRequest(page_url, method="GET", callback = self.parse_items)

  def parse_items(self, response):
    for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[1], url)
      yield FormRequest(item_url, method="GET", callback = self.parse_summary_result)

  def parse_summary_result(self, response):
    # inspect_response(response)
    self.item += response.xpath("//table[@id='simpleDetailsTable']//tr/td/text()").extract()
    self.item = [re.sub(r"\s+", " ", " " + itr + " ").strip() for itr in self.item]

    # import ipdb as d; d.set_trace()

    # for it in xrange(self.item):
    #   group = self.items.setdefault(item[item], [])
    #   try:
    #     group.append(item[1]) or "N/A"
    #   try:
    #     group.append(item[2]) or "N/A"

    # import ipdb as d; d.set_trace()

    detail_url = response.xpath("//*[@id='subtab_details']/@href").extract()[0]
    detail_url = '{0}{1}'.format(self.base_url[1], detail_url)
    request = FormRequest(detail_url, method = "GET", callback = self.parse_detail_result)
    request.meta['item_content'] = self.item
    return request

  def parse_detail_result(self, response):
    inspect_response(response)



  # def parse_date_result(self, response):
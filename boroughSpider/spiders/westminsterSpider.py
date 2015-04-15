from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request,FormRequest
from boroughSpider.items import idoxpaItem
from scrapy import log
import urllib, re, time, json

today = time.strftime("%x %X")

class idoxpaSpider(Spider):
  name = 'idoxpaSpider'

  pipeline = 'idoxpaPipeline'

  domain = 'www.westminster.gov.uk'

  base_url = ["http://idoxpa.westminster.gov.uk/online-applications/pagedSearchResults.do?action=page&searchCriteria.page=",
              "http://idoxpa.westminster.gov.uk"]

  start_urls = ["http://idoxpa.westminster.gov.uk/online-applications/search.do?action=monthlyList"]

  def parse(self, response):
    for parish in response.xpath("//*[@id='parish']/option/@value").extract()[1:]:
      for month in response.xpath("//*[@id='month']/option/text()").extract():
        yield FormRequest.from_response(response,
                          formname = 'searchCriteriaForm',
                          formdata = { 'searchCriteria.parish':parish,
                                       'month':month,
                                       'dateType':'DC_Validated',
                                       'searchType':'Application' },
                          callback = self.parse_results)

  def parse_results(self, response):
    # inspect_response(response)
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
    # inspect_response(response)
    item = idoxpaItem()

    td = []
    # td += response.xpath("//table[@id='simpleDetailsTable']//tr/td/text()").extract()
    td = [''.join(text.xpath('.//text()').extract()) for text in response.xpath("//table[@id='simpleDetailsTable']//tr/td")]
    td = [re.sub(r"\s+", " ", " " + itr + " ").strip() for itr in td]

    item['case_reference'] = td[0]
    item['application_received_date'] = td[2]
    item['application_validation_date'] = td[3]
    item['address'] = td[4]
    item['proposed_development'] = td[5]
    item['status'] = td[6]
    item['decision'] = td[7]
    item['appeal_status'] = td[8]
    item['appeal_decision'] = td[9]

    further_info_url = response.xpath("//*[@id='subtab_details']/@href").extract()[0]
    further_info_url = '{0}{1}'.format(self.base_url[1], further_info_url)
    request = FormRequest(further_info_url, method = "GET",
                          meta = {'item':item},
                          callback = self.parse_further_info)
    return request

  def parse_further_info(self, response):
    inspect_response(response)
    item = response.meta['item']

    td = []
    #td += response.xpath("//table[@id='applicationDetails']//tr/td/text()").extract()
    td = [''.join(text.xpath('.//text()').extract()) for text in response.xpath("//table[@id='applicationDetails']//tr/td")]
    td = [re.sub(r"\s+", " ", " " + itr + " ").strip() for itr in td]

    item['application_type'] = td[0]
    item['planning_case_officer'] = td[2]
    item['amenity_society'] = td[3]
    item['ward'] = td[4]
    item['district_reference'] = td[5]
    item['applicants_name'] = td[6]
    item['agent_name'] = td[7]
    item['agency_company_name'] = td[8]
    item['agent_address'] = td[9]
    item['environmental_assessment_requested'] = td[10]


    important_dates_url = response.xpath("//*[@id='subtab_dates']/@href").extract()[0]
    important_dates_url = '{0}{1}'.format(self.base_url[1], important_dates_url)
    request = FormRequest(important_dates_url, method = "GET",
                          meta = {'item':item},
                          callback = self.parse_important_dates)
    return request

  def parse_important_dates(self, response):
    item = response.meta['item']

    td = []
    # td += response.xpath("//table[@id='simpleDetailsTable']//tr/td/text()").extract()
    td = [''.join(text.xpath('.//text()').extract()) for text in response.xpath("//table[@id='simpleDetailsTable']//tr/td")]
    td = [re.sub(r"\s+", " ", " " + itr + " ").strip() for itr in td]

    item['application_received_date'] = td[0]
    item['application_validated_date'] = td[1]
    item['decision_date'] = td[8]
    item['target_date'] = td[10]

    try:
      documents_url = response.xpath("//*[@id='tab_documents']/@href").extract()[0]
      documents_url = '{0}{1}'.format(self.base_url[1], documents_url)
      item['documents_url'] = documents_url
    except:
      item['documents_url'] = "N/A"

    return item
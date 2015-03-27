from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request,FormRequest
from scrapy.exceptions import CloseSpider
from boroughSpider.items import ApplicationItem
from scrapy import log
import urllib, re, time

today = time.strftime("%x %X")

class ibhfSpider(Spider):
  name = 'ibhfSpider'
  domain = 'http://ibhf.gov.uk'

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
    for page_num in xrange(1, num_of_pages+1):
      page_url = '{0}{1}'.format(self.base_url[0], num_of_pages)
      yield FormRequest(page_url, method="GET", callback = self.parse_items)

  def parse_items(self, response):
    for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[1], url)
      yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_summary(self, response):
    # inspect_response(response)
    item = ApplicationItem()

    td = []
    td += response.xpath("//table[@id='simpleDetailsTable']//tr/td/text()").extract()
    td = [re.sub(r"\s+", " ", " " + itr + " ").strip() for itr in td]

    item['case_reference'] = td[0]
    item['planning_portal_reference'] = td[1]
    item['application_registration'] = td[2]
    item['application_validation'] = td[3]
    item['address'] = td[4]
    item['proposed_development'] = td[5]
    item['application_status'] = td[6]
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
    #inspect_response(response)
    item = response.meta['item']

    td = []
    td += response.xpath("//table[@id='applicationDetails']//tr/td/text()").extract()
    td = [re.sub(r"\s+", " ", " " + itr + " ").strip() for itr in td]

    item['application_type'] = td[0]
    item['expected_decision_level'] = td[1]
    item['planning_case_officer'] = td[2]
    item['ward'] = td[3]
    item['applicants_name'] = td[4]
    item['agent_name'] = td[5]
    item['agency_company_name'] = td[6]
    item['environmental_assessment_requested'] = td[7]


    important_dates_url = response.xpath("//*[@id='subtab_dates']/@href").extract()[0]
    important_dates_url = '{0}{1}'.format(self.base_url[1], important_dates_url)
    request = FormRequest(important_dates_url, method = "GET",
                          meta = {'item':item},
                          callback = self.parse_important_dates)
    return request

  def parse_important_dates(self, response):
    #inspect_response(response)
    item = response.meta['item']

    td = []
    td += response.xpath("//table[@id='simpleDetailsTable']//tr/td/text()").extract()
    td = [re.sub(r"\s+", " ", " " + itr + " ").strip() for itr in td]

    item['closing_date_for_comments'] = td[1]
    item['statutory_expiry_date'] = td[2]
    item['agreed_expiry_date'] = td[3]
    item['permission_expiry_date'] = td[5]
    item['temporary_permission_expiry_date'] = td[6]

    constraint_url = response.xpath("//*[@id='tab_constraints']/@href").extract()
    constraint_url = '{0}{1}'.format(self.base_url[1], constraint_url)
    request = FormRequest(constraint_url, method = "GET",
                          meta = {'item':item},
                          callback = self.parse_constraints)
    return request

  def parse_constraints(self, response):
    inspect_response(response)
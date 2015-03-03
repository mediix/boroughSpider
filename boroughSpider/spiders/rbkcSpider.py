from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request,FormRequest
from scrapy.exceptions import CloseSpider
from boroughSpider.items import ApplicationItem
from scrapy import log
import csv, urllib, re, time, sys

today = time.strftime("%x %X")

class rkbcSpider(Spider):
    name = 'rbkcSpider'
    domain = "rbkc.gov.uk"

    def __init__(self):
        self.allowed_domains = ["rbkc.gov.uk"]
        self.base_url = ["http://www.rbkc.gov.uk/planning/scripts/weeklyform.asp",
                         "http://www.rbkc.gov.uk/planning/scripts/weeklyresults.asp"]

    def start_requests(self):
        Req = []
        Req.append(FormRequest(self.base_url[0], method="GET", callback = self.parse_date_result))
        return Req

    def parse_date_result(self, response):
        Req = []
        for date in response.xpath("//select[@id='WeekEndDate']/option/@value").extract():
            Req.append(FormRequest(self.base_url[1], method="POST", formdata={ 'WeekEndDate':date, 'order':'Received Date' }, callback = self.parse_search_result))
        return Req

    def parse_search_result(self, response):
        Req = []
        for href in response.xpath("//table[@id='Table1']//td/a/@href").extract():
            Req.append(FormRequest(self.domain + href, method="GET", callback = self.parse_item_result))
        return Req

    def parse_item_result(self, response):
        item = ApplicationItem()
        th = []
        td = []

        # property-details
        th += response.xpath("//table[@id='property-details']//tr/th/text()").extract()
        td += response.xpath("//table[@id='property-details']//tr/td/text()").extract()

        # applicant-details
        th += response.xpath("//table[@id='applicant-details']//tr/th/text()").extract()
        td += response.xpath("//table[@id='applicant-details']//tr/td/text()").extract()

        # proposal-details
        th += response.xpath("//table[@id='proposal-details']//tr/th/text()").extract()
        td += response.xpath("//table[@id='proposal-details']//tr/td/text()").extract()

        # decision-details
        th += response.xpath("//table[@id='decision-details']//tr/th/text()").extract()
        td += response.xpath("//table[@id='decision-details']//tr/td/text()").extract()

        # appeal-details
        th += response.xpath("//table[@id='appeal-details']//tr/th/text()").extract()
        td += response.xpath("//table[@id='appeal-details']//tr/td/text()").extract()

        # planning-dept-contact
        th += response.xpath("//table[@id='planning-dept-contact']//tr/th/text()").extract()
        td += response.xpath("//table[@id='planning-dept-contact']//tr/td/text()").extract()


        item["borough"] = "Royal Borough of Kensington and Chelsea"
        item["domain"] = "rbkc.uk.gov"
        #
        url = response.xpath("//a[@href='#tabs-planning-6']/@href").extract()
        item["documents_url"] = response.url + url[0]

        # Filling item fields
        item["Case_reference"] = record[0] or "N/A"
        item["Address"] = record[1] or "N/A"
        item["Ward"] = record[2] or "N/A"
        item["Polling_district"] = record[3] or "N/A"
        item["Listed_building_grade"] = record[4] or "N/A"
        item["Conservation_area"] = record[5] or "N/A"
        item["Application_name"] = record[6] or "N/A"
        item["Contact_name"] = record[7] or "N/A"
        item["Contact_address"] = record[8] or "N/A"
        item["Contact_telephone"] = record[9] or "N/A"
        item["Application_type"] = record[10] or "N/A"
        item["Proposed_development"] = record[11] or "N/A"
        item["Date_received"] = record[12] or "N/A"
        item["Registration_date"] = record[13] or "N/A"
        item["Public_consultation_ends"] = record[14] or "N/A"
        item["Application_status"] = record[15] or "N/A"
        item["Target_date_for_decision"] = record[16] or "N/A"
        item["Decision"] = record[17] or "N/A"
        item["Decision_date"] = record[18] or "N/A"
        item["Conditions_and_reasons"] = record[19] or "N/A"
        item["Date_scrapped"] = today
        item["Domain"] = self.domain
        return item

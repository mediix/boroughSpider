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
    domain = 'http://rbkc.gov.uk'

    def __init__(self):
        self.allowed_domains = ["rbkc.gov.uk"]
        self.base_url = ["http://www.rbkc.gov.uk/planning/scripts/weeklyform.asp",
                         "http://www.rbkc.gov.uk/planning/scripts/weeklyresults.asp"]

    def start_requests(self):
        Req = []
        Req.append(FormRequest(self.base_url[0], method="GET", callback = self.parse_date_result))
        return Req

    def parse_date_result(self, response):
        #inspect_response(response)
        Req = []
        for date in response.xpath("//select[@id='WeekEndDate']/option/@value").extract():
            Req.append(FormRequest(self.base_url[1], method="POST", formdata={ 'WeekEndDate':date, 'order':'Received Date' }, callback = self.parse_search_result))
        return Req

    def parse_search_result(self, response):
        #inspect_response(response)
        Req = []
        for href in response.xpath("//table[@id='Table1']//td/a/@href").extract():
            Req.append(FormRequest(self.domain + href, method="GET", callback = self.parse_item_result))
        return Req

    def parse_item_result(self, response):
        #inspect_response(response)
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
        th += response.xpath("//table[@id='proposal-details']//tr/th/text()[not(preceding-sibling::br)]").extract()
        td += response.xpath("//table[@id='proposal-details']//tr/td/text()[not(preceding-sibling::br)]").extract()

        # decision-details
        th += response.xpath("//table[@id='decision-details']//tr/th/text()").extract()
        td += response.xpath("//table[@id='decision-details']//tr/td/text()").extract()

        # appeal-details
        th += response.xpath("//table[@id='appeal-details']//tr/th/text()").extract()
        td += response.xpath("//table[@id='appeal-details']//tr/td/text()").extract()

        # planning-dept-contact
        th += response.xpath("//table[@id='planning-dept-contact']//tr/th/text()").extract()
        td += response.xpath("//table[@id='planning-dept-contact']//tr/td/text()").extract()


        th = [re.sub(r"\s+", " ", " " + itr + " ").strip() for itr in th]
        td = [re.sub(r"\s+", " ", " " + itr + " ").strip() for itr in td]


        item["borough"] = "Royal Borough of Kensington and Chelsea"
        item["domain"] = "rbkc.uk.gov"

        # Filling item fields
        try:
            item['case_reference'] = td[0]
        except:
            item['case_reference'] = "n/a"
        try:
            item["address"] = td[1]
        except:
            item["address"] = "n/a"
        try:
            item["ward"] = td[2]
        except:
            item["ward"] = "n/a"
        try:
            item["polling_district"] = td[3]
        except:
            item["polling_district"] = "n/a"
        try:
            item["listed_building_grade"] = td[4]
        except:
            item["listed_building_grade"] = "n/a"
        try:
            item["conservation_area"] = td[5]
        except:
            item["conservation_area"] = "n/a"
        try:
            item["applicants_name"] = td[6]
        except:
            item["applicants_name"] = "n/a"
        try:
            item["contact_name"] = td[7]
        except:
            item["contact_name"] = "n/a"
        try:
            item["contact_address"] = td[8]
        except:
            item["contact_address"] = "n/a"
        try:
            item["contact_telephone"] = td[9]
        except:
            item["contact_telephone"] = "n/a"
        try:
            item["application_type"] = td[10]
        except:
            item["application_type"] = "n/a"
        try:
            item["proposed_development"] = td[11]
        except:
            item["proposed_development"] = "n/a"
        try:
            item["date_received"] = td[12]
        except:
            item["date_received"] = "n/a"
        try:
            item["registration_date"] = td[13]
        except:
            item["registration_date"] = "n/a"
        try:
            item["public_consultation_ends"] = td[14]
        except:
            item["public_consultation_ends"] = "n/a"
        try:
            item["application_status"] = td[15]
        except:
            item["application_status"] = "n/a"
        try:
            item["target_date_for_decision"] = td[16]
        except:
            item["target_date_for_decision"] = "n/a"
        try:
            item["decision"] = td[17]
        except:
            item["decision"] = "n/a"
        try:
            item["decision_date"] = td[18]
        except:
            item["decision_date"] = "n/a"
        try:
            item["conditions_and_reasons"] = td[19]
        except:
            item["conditions_and_reasons"] = "n/a"
        try:
            item["formal_reference_number"] = td[20]
        except:
            item["formal_reference_number"] = "n/a"
        try:
            item["appeal_received"] = td[21]
        except:
            item["appeal_received"] = "n/a"
        try:
            item["appeal_start_date"] = td[22]
        except:
            item["appeal_start_date"] = "n/a"
        try:
            item["appeal_decision"] = td[23]
        except:
            item["appeal_decision"] = "n/a"
        try:
            item["appeal_decision_date"] = td[24]
        except:
            item["appeal_decision_date"] = "n/a"
        try:
            item["planning_case_officer"] = td[25]
        except:
            item["planning_case_officer"] = "n/a"
        try:
            item["planning_team"] = td[26]
        except:
            item["planning_team"] = "n/a"

        url = response.xpath("//a[@href='#tabs-planning-6']/@href").extract()
        item["documents_url"] = response.url + url[0]

        return item


        '''
        item["Case_reference"] = td[0] or "N/A"
        item["Address"] = td[1] or "N/A"
        item["Ward"] = td[2] or "N/A"
        item["Polling_district"] = td[3] or "N/A"
        item["Listed_building_grade"] = td[4] or "N/A"
        item["Conservation_area"] = td[5] or "N/A"
        item["Application_name"] = td[6] or "N/A"
        item["Contact_name"] = td[7] or "N/A"
        item["Contact_address"] = td[8] or "N/A"
        item["Contact_telephone"] = td[9] or "N/A"
        item["Application_type"] = td[10] or "N/A"
        item["Proposed_development"] = td[11] or "N/A"
        item["Date_received"] = td[12] or "N/A"
        item["Registration_date"] = td[13] or "N/A"
        item["Public_consultation_ends"] = td[14] or "N/A"
        item["Application_status"] = td[15] or "N/A"
        item["Target_date_for_decision"] = td[16] or "N/A"
        item["Decision"] = td[17] or "N/A"
        item["Decision_date"] = td[18] or "N/A"
        item["Conditions_and_reasons"] = td[19] or "N/A"
        item["formal_reference_number"] = td[20] or "N/A"
        item["appeal_received"] = td[21] or "N/A"
        item["appeal_start_date"] = td[22] or "N/A"
        item["appeal_decision"] = td[23] or "N/A"
        item["appeal_decision_date"] = td[24] or "N/A"
        item["planning_case_office"] = td[25] or "N/A"
        item["planning_team"] = td[26] or "N/A"
        '''

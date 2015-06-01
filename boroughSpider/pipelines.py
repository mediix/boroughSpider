# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy import log
import MySQLdb
import functools
import sys

def check_spider_pipeline(process_item_method):

    @functools.wraps(process_item_method)
    def wrapper(self, item, spider):
        msg = '%%s %s pipeline' % (self.__class__.__name__)
        if self.__class__.__name__ == spider.pipeline:
            spider.log(msg % 'executing', level=log.DEBUG)
            return process_item_method(self, item, spider)
        else:
            # spider.log(msg % 'skipping', level=log.DEBUG)
            return item
    return wrapper

class Kensington(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
            self.cursor.execute("""SET @address_id = LAST_INSERT_ID();""")
            self.cursor.execute("""INSERT INTO boroughs
            (address_id,
            borough,
            domain,
            case_reference,
            ward,
            polling_district,
            listed_building_grade,
            conservation_area,
            applicants_name,
            contact_name,
            contact_address,
            contact_telephone,
            application_type,
            proposed_development,
            date_received,
            registration_date,
            public_consultation_ends,
            application_status,
            target_date_for_decision,
            decision,
            decision_date,
            conditions_and_reasons,
            formal_reference_number,
            appeal_received,
            appeal_start_date,
            appeal_decision,
            appeal_decision_date,
            planning_case_officer,
            planning_team,
            documents_url,
            date_scraped) VALUES (@address_id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, NOW())""",
            (item.get('borough', self.default),
            item.get('domain', self.default),
            item.get('case_reference', self.default),
            item.get('ward', self.default),
            item.get('polling_district', self.default),
            item.get('listed_building_grade', self.default),
            item.get('conservation_area', self.default),
            item.get("applicant's_name", self.default),
            item.get('contact_name', self.default),
            item.get('contact_address', self.default),
            item.get('contact_telephone', self.default),
            item.get('application_type', self.default),
            item.get('proposed_development', self.default),
            item.get('date_received', self.default),
            item.get('registration_date_(statutory_start_date)', self.default),
            item.get('public_consultation_ends', self.default),
            item.get('application_status', self.default),
            item.get('target_date_for_decision', self.default),
            item.get('decision', self.default),
            item.get('decision_date', self.default),
            item.get('conditions_and_reasons', self.default),
            item.get('formal_reference_number', self.default),
            item.get('appeal_received', self.default),
            item.get('appeal_start_date', self.default),
            item.get('appeal_decision', self.default),
            item.get('appeal_decision_date', self.default),
            item.get('planning_case_officer', self.default),
            item.get('planning_team', self.default),
            item.get('documents_url', self.default)))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item


class Hammersmith(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='192.168.1.207', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
            self.cursor.execute("""SET @address_id = LAST_INSERT_ID();""")
            self.cursor.execute("""INSERT INTO boroughs
            (address_id,
            borough,
            domain,
            case_reference,
            ward,
            applicants_name,
            application_type,
            proposed_development,
            application_status,
            decision,
            decision_date,
            appeal_decision,
            appeal_decision_date,
            documents_url,
            planning_portal_reference,
            application_registration,
            application_validation,
            appeal_status,
            expected_decision_level,
            agent_name,
            agency_company_name,
            environmental_assessment_requested,
            closing_date_for_comments,
            statutory_expiry_date,
            agreed_expiry_date,
            permission_expiry_date,
            temporary_permission_expiry_date,
            constraints,
            date_scraped) VALUES (@address_id, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());""",
            (item.get('borough', self.default),
            item.get('domain', self.default),
            item.get('reference', self.default),
            item.get('ward', self.default),
            item.get('applicant_name', self.default),
            item.get('application_type', self.default),
            item.get('proposal', self.default),
            item.get('status', self.default),
            item.get('decision', self.default),
            item.get('decision_date', self.default),
            item.get('appeal_decision', self.default),
            item.get('appeal_status', self.default),
            item.get('documents_url', self.default),
            item.get('planning_portal_reference', self.default),
            item.get('application_registered', self.default),
            item.get('application_validated', self.default),
            item.get('appeal_status', self.default),
            item.get('expected_decision_level', self.default),
            item.get('agent_name', self.default),
            item.get('agent_company_name', self.default),
            item.get('environmental_assessment_requested', self.default),
            item.get('closing_date_for_comments', self.default),
            item.get('statutory_expiry_date', self.default),
            item.get('agreed_expiry_date', self.default),
            item.get('permission_expiry_date', self.default),
            item.get('temporary_permission_expiry_date', self.default),
            item.get('constraints', self.default)))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item

class Westminster(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
            self.cursor.execute("""SET @address_id = LAST_INSERT_ID();""")
            self.cursor.execute("""INSERT INTO boroughs
            (address_id,
            borough,
            domain,
            case_reference,
            ward,
            planning_case_officer,
            proposed_development,
            application_status,
            decision,
            appeal_decision,
            appeal_status,
            application_type,
            amenity_society,
            district_reference,
            applicants_name,
            contact_address,
            agent_name,
            agency_company_name,
            agent_address,
            environmental_assessment_requested,
            application_received_date,
            application_validated_date,
            documents_url,
            date_scraped) VALUES (@address_id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (item.get('borough', self.default),
            item.get('domain', self.default),
            item.get('reference', self.default),
            item.get('ward', self.default),
            item.get('case_officer', self.default),
            item.get('proposal', self.default),
            item.get('status', self.default),
            item.get('decision', self.default),
            item.get('appeal_decision', self.default),
            item.get('appeal_status', self.default),
            item.get('application_type', self.default),
            item.get('amenity_society', self.default),
            item.get('district_reference', self.default),
            item.get('applicant_name', self.default),
            item.get('applicant_address', self.default),
            item.get('case_officer', self.default),
            item.get('agent_company_name', self.default),
            item.get('agent_address', self.default),
            item.get('environmental_assessment_requested', self.default),
            item.get('application_received', self.default),
            item.get('application_validated', self.default),
            item.get('documents_url', self.default)))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item


class CityOfLondon(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
            self.cursor.execute("""SET @address_id = LAST_INSERT_ID();""")
            self.cursor.execute("""INSERT INTO boroughs
            (address_id,
            borough,
            domain,
            case_reference,
            ward,
            applicants_name,
            application_type,
            proposed_development,
            application_status,
            decision,
            decision_date,
            appeal_decision,
            appeal_decision_date,
            documents_url,
            planning_portal_reference,
            application_registration,
            application_validation,
            appeal_status,
            expected_decision_level,
            agent_name,
            agency_company_name,
            environmental_assessment_requested,
            closing_date_for_comments,
            statutory_expiry_date,
            agreed_expiry_date,
            permission_expiry_date,
            temporary_permission_expiry_date,
            constraints,
            date_scraped) VALUES (@address_id, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());""",
            (item.get('borough', self.default),
            item.get('domain', self.default),
            item.get('reference', self.default),
            item.get('ward', self.default),
            item.get('applicant_name', self.default),
            item.get('application_type', self.default),
            item.get('proposal', self.default),
            item.get('status', self.default),
            item.get('decision', self.default),
            item.get('decision_date', self.default),
            item.get('appeal_decision', self.default),
            item.get('appeal_status', self.default),
            item.get('documents_url', self.default),
            item.get('planning_portal_reference', self.default),
            item.get('application_registered', self.default),
            item.get('application_validated', self.default),
            item.get('appeal_status', self.default),
            item.get('expected_decision_level', self.default),
            item.get('agent_name', self.default),
            item.get('agent_company_name', self.default),
            item.get('environmental_assessment_requested', self.default),
            item.get('closing_date_for_comments', self.default),
            item.get('statutory_expiry_date', self.default),
            item.get('agreed_expiry_date', self.default),
            item.get('permission_expiry_date', self.default),
            item.get('temporary_permission_expiry_date', self.default),
            item.get('constraints', self.default)))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item

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
        msg = '%%s %s pipeline step' % (self.__class__.__name__)
        if self.__class__.__name__ == spider.pipeline:
            spider.log(msg % 'executing', level=log.DEBUG)
            return process_item_method(self, item, spider)
        else:
            spider.log(msg % 'skipping', level=log.DEBUG)
            return item
    return wrapper

class Kensington(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='root', passwd='pashmak.', db='scrapy', host='rappi.local', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO boroughs
            (borough,
            domain,
            case_reference,
            address,
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
            date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, NOW())""",
            (item['borough'].encode('utf-8'),
            item['domain'].encode('utf-8'),
            item['case_reference'].encode('utf-8'),
            item['address'].encode('utf-8'),
            item['ward'].encode('utf-8'),
            item['polling_district'].encode('utf-8'),
            item['listed_building_grade'].encode('utf-8'),
            item['conservation_area'].encode('utf-8'),
            item["applicant's_name"].encode('utf-8'),
            item['contact_name'].encode('utf-8'),
            item['contact_address'].encode('utf-8'),
            item['contact_telephone'].encode('utf-8'),
            item['application_type'].encode('utf-8'),
            item['proposed_development'].encode('utf-8'),
            item['date_received'].encode('utf-8'),
            item['registration_date_(statutory_start_date)'].encode('utf-8'),
            item['public_consultation_ends'].encode('utf-8'),
            item['application_status'].encode('utf-8'),
            item['target_date_for_decision'].encode('utf-8'),
            item['decision'].encode('utf-8'),
            item['decision_date'].encode('utf-8'),
            item['conditions_and_reasons'].encode('utf-8'),
            item['formal_reference_number'].encode('utf-8'),
            item['appeal_received'].encode('utf-8'),
            item['appeal_start_date'].encode('utf-8'),
            item['appeal_decision'].encode('utf-8'),
            item['appeal_decision_date'].encode('utf-8'),
            item['planning_case_officer'].encode('utf-8'),
            item['planning_team'].encode('utf-8'),
            item['documents_url'].encode('utf-8')))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item


class Hammersmith(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='root', passwd='pashmak.', db='scrapy', host='rappi.local', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO boroughs
            (borough,
            domain,
            case_reference,
            address,
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
            date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (item['borough'].encode('utf-8'),
            item['domain'].encode('utf-8'),
            item['case_reference'].encode('utf-8'),
            item['address'].encode('utf-8'),
            item['ward'].encode('utf-8'),
            item['polling_district'].encode('utf-8'),
            item['listed_building_grade'].encode('utf-8'),
            item['conservation_area'].encode('utf-8'),
            item['applicants_name'].encode('utf-8'),
            item['contact_name'].encode('utf-8'),
            item['contact_address'].encode('utf-8'),
            item['contact_telephone'].encode('utf-8'),
            item['application_type'].encode('utf-8'),
            item['proposed_development'].encode('utf-8'),
            item['date_received'].encode('utf-8'),
            item['registration_date'].encode('utf-8'),
            item['public_consultation_ends'].encode('utf-8'),
            item['application_status'].encode('utf-8'),
            item['target_date_for_decision'].encode('utf-8'),
            item['decision'].encode('utf-8'),
            item['decision_date'].encode('utf-8'),
            item['conditions_and_reasons'].encode('utf-8'),
            item['formal_reference_number'].encode('utf-8'),
            item['appeal_received'].encode('utf-8'),
            item['appeal_start_date'].encode('utf-8'),
            item['appeal_decision'].encode('utf-8'),
            item['appeal_decision_date'].encode('utf-8'),
            item['planning_case_officer'].encode('utf-8'),
            item['planning_team'].encode('utf-8'),
            item['documents_url'].encode('utf-8'),
            item['planning_portal_reference'].encode('utf-8'),
            item['application_registration'].encode('utf-8'),
            item['application_validation'].encode('utf-8'),
            item['appeal_status'].encode('utf-8'),
            item['expected_decision_level'].encode('utf-8'),
            item['agent_name'].encode('utf-8'),
            item['agency_company_name'].encode('utf-8'),
            item['environmental_assessment_requested'].encode('utf-8'),
            item['closing_date_for_comments'].encode('utf-8'),
            item['statutory_expiry_date'].encode('utf-8'),
            item['agreed_expiry_date'].encode('utf-8'),
            item['permission_expiry_date'].encode('utf-8'),
            item['temporary_permission_expiry_date'].encode('utf-8'),
            item['constraints'].encode('utf-8')))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item

class Westminster(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='root', passwd='pashmak.', db='scrapy', host='rappi.local', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO boroughs
            (borough,
            domain,
            case_reference,
            address,
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
            date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (item['borough'].encode('utf-8'),
            item['domain'].encode('utf-8'),
            item['reference'].encode('utf-8'),
            item['address'].encode('utf-8'),
            item['ward'].encode('utf-8'),
            item['case_officer'].encode('utf-8'),
            item['proposal'].encode('utf-8'),
            item['status'].encode('utf-8'),
            item['decision'].encode('utf-8'),
            item['appeal_decision'].encode('utf-8'),
            item['appeal_status'].encode('utf-8'),
            item['application_type'].encode('utf-8'),
            item['amenity_society'].encode('utf-8'),
            item['district_reference'].encode('utf-8'),
            item['applicant_name'].encode('utf-8'),
            item['applicant_address'].encode('utf-8'),
            item['case_officer'].encode('utf-8'),
            item['agent_company_name'].encode('utf-8'),
            item['agent_address'].encode('utf-8'),
            item['environmental_assessment_requested'].encode('utf-8'),
            item['application_received'].encode('utf-8'),
            item['application_validated'].encode('utf-8'),
            item['documents_url'].encode('utf-8')))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item


class CityOfLondon(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='root', passwd='pashmak.', db='scrapy', host='rappi.local', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO boroughs
            (borough,
            domain,
            case_reference,
            address,
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
            date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (item['borough'].encode('utf-8'),
            item['domain'].encode('utf-8'),
            item['reference'].encode('utf-8'),
            item['address'].encode('utf-8'),
            item['ward'].encode('utf-8'),
            item['case_officer'].encode('utf-8'),
            item['proposal'].encode('utf-8'),
            item['status'].encode('utf-8'),
            item['decision'].encode('utf-8'),
            item['appeal_decision'].encode('utf-8'),
            item['appeal_status'].encode('utf-8'),
            item['application_type'].encode('utf-8'),
            item['amenity_society'].encode('utf-8'),
            item['district_reference'].encode('utf-8'),
            item['applicant_name'].encode('utf-8'),
            item['applicant_address'].encode('utf-8'),
            item['case_officer'].encode('utf-8'),
            item['agent_company_name'].encode('utf-8'),
            item['agent_address'].encode('utf-8'),
            item['environmental_assessment_requested'].encode('utf-8'),
            item['application_received'].encode('utf-8'),
            item['application_validated'].encode('utf-8'),
            item['documents_url'].encode('utf-8')))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item

# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import functools
import sys
import re
import pickle
import os
from scrapy import log

file_path = "/home/medi/workbench/boroughSpider/mappings/NAME_item_keys.pkl"

def check_spider_pipeline(process_item_method):
    @functools.wraps(process_item_method)
    def wrapper(self, item, spider):
        msg = '%%s %s pipeline' % (self.__class__.__name__)
        if self.__class__.__name__ in spider.pipeline:
            spider.log(msg % 'executing', level=log.DEBUG)
            return process_item_method(self, item, spider)
        else:
            # spider.log(msg % 'skipping', level=log.DEBUG)
            return item
    return wrapper

def store_keys(item, name):
    """"""
    file_name = file_path.replace('NAME', name)
    item_keys = item.keys()

    if not os.path.isfile(file_name):
        with open(file_name, 'wb') as f:
            pickle.dump(item_keys, f)
    try:
        with open(file_name, 'rb') as f:
            stored_keys = pickle.load(f)
    except Exception as err:
        print "Error opening file: ", err
        pass
    if stored_keys:
        diff = list(set(item_keys) - set(stored_keys))
        if diff:
            print "\n!!!!!!!!DIFF IS: \n", diff
            stored_keys = stored_keys + diff
            with open(file_name, 'wb') as f:
                pickle.dump(stored_keys, f)
    else:
        stored_keys = item_keys
        with open(file_name, 'wb') as f:
            pickle.dump(stored_keys, f)

class Kensington(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'

    @check_spider_pipeline
    def process_item(self, item, spider):
        sp_name = self.__class__.__name__
        store_keys(item, sp_name)
        with open(file_path.replace('NAME', sp_name), 'rb') as f:
            db_keymap = pickle.load(f)

        data = {}
        for key, value in db_keymap.items():
            data[key] = item.get(key)

        data = [tuple(item.get(db_keymap.get(key)) for key in db_keymap.keys())]

        # try:
        #     self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
        #     address_response = self.cursor.fetchone()
        #     if address_response is None:
        #         self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
        #         self.conn.commit()
        #         self.cursor.execute("SELECT LAST_INSERT_ID();")
        #         address_response = self.cursor.fetchone()

        #     address_id = address_response[0]

        #     self.cursor.execute("""INSERT INTO boroughs
        #     (address_id,
        #     borough,
        #     domain,
        #     case_reference,
        #     ward,
        #     polling_district,
        #     listed_building_grade,
        #     conservation_area,
        #     applicants_name,
        #     contact_name,
        #     contact_address,
        #     contact_telephone,
        #     application_type,
        #     proposed_development,
        #     date_received,
        #     registration_date,
        #     public_consultation_ends,
        #     application_status,
        #     target_date_for_decision,
        #     decision,
        #     decision_date,
        #     conditions_and_reasons,
        #     formal_reference_number,
        #     appeal_received,
        #     appeal_start_date,
        #     appeal_decision,
        #     appeal_decision_date,
        #     planning_case_officer,
        #     planning_team,
        #     documents_url,
        #     date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        #                                 %s, %s, NOW())""",
        #     (address_id,
        #     item.get('borough', self.default),
        #     item.get('domain', self.default),
        #     item.get('case_reference', self.default),
        #     item.get('ward', self.default),
        #     item.get('polling_district', self.default),
        #     item.get('listed_building_grade', self.default),
        #     item.get('conservation_area', self.default),
        #     item.get("applicant's_name", self.default),
        #     item.get('contact_name', self.default),
        #     item.get('contact_address', self.default),
        #     item.get('contact_telephone', self.default),
        #     item.get('application_type', self.default),
        #     item.get('proposed_development', self.default),
        #     item.get('date_received', self.default),
        #     item.get('registration_date_(statutory_start_date)', self.default),
        #     item.get('public_consultation_ends', self.default),
        #     item.get('application_status', self.default),
        #     item.get('target_date_for_decision', self.default),
        #     item.get('decision', self.default),
        #     item.get('decision_date', self.default),
        #     item.get('conditions_and_reasons', self.default),
        #     item.get('formal_reference_number', self.default),
        #     item.get('appeal_received', self.default),
        #     item.get('appeal_start_date', self.default),
        #     item.get('appeal_decision', self.default),
        #     item.get('appeal_decision_date', self.default),
        #     item.get('planning_case_officer', self.default),
        #     item.get('planning_team', self.default),
        #     item.get('documents_url', self.default)))
        #     self.conn.commit()
        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     return item


class Hammersmith(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'
        self.path = "/home/medi/workbench/boroughSpider/NAME_item_keys.pkl"

    @check_spider_pipeline
    def process_item(self, item, spider):
        store_keys(item, self.__class__.__name__)
        # try:
        #     self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
        #     address_response = self.cursor.fetchone()
        #     if address_response is None:
        #         self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
        #         self.conn.commit()
        #         self.cursor.execute("SELECT LAST_INSERT_ID();")
        #         address_response = self.cursor.fetchone()

        #     address_id = address_response[0]

        #     self.cursor.execute("""INSERT INTO boroughs
        #     (address_id,
        #     borough,
        #     domain,
        #     case_reference,
        #     ward,
        #     applicants_name,
        #     application_type,
        #     proposed_development,
        #     application_status,
        #     decision,
        #     decision_date,
        #     appeal_decision,
        #     appeal_decision_date,
        #     documents_url,
        #     planning_portal_reference,
        #     application_registration,
        #     application_validation,
        #     appeal_status,
        #     expected_decision_level,
        #     agent_name,
        #     agency_company_name,
        #     environmental_assessment_requested,
        #     closing_date_for_comments,
        #     statutory_expiry_date,
        #     agreed_expiry_date,
        #     permission_expiry_date,
        #     temporary_permission_expiry_date,
        #     constraints,
        #     date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
        #                                         %s, %s, %s, %s, %s, %s, %s, %s,
        #                                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());""",
        #     (address_id,
        #     item.get('borough', self.default),
        #     item.get('domain', self.default),
        #     item.get('reference', self.default),
        #     item.get('ward', self.default),
        #     item.get('applicant_name', self.default),
        #     item.get('application_type', self.default),
        #     item.get('proposal', self.default),
        #     item.get('status', self.default),
        #     item.get('decision', self.default),
        #     item.get('decision_date', self.default),
        #     item.get('appeal_decision', self.default),
        #     item.get('appeal_status', self.default),
        #     item.get('documents_url', self.default),
        #     item.get('planning_portal_reference', self.default),
        #     item.get('application_registered', self.default),
        #     item.get('application_validated', self.default),
        #     item.get('appeal_status', self.default),
        #     item.get('expected_decision_level', self.default),
        #     item.get('agent_name', self.default),
        #     item.get('agent_company_name', self.default),
        #     item.get('environmental_assessment_requested', self.default),
        #     item.get('closing_date_for_comments', self.default),
        #     item.get('statutory_expiry_date', self.default),
        #     item.get('agreed_expiry_date', self.default),
        #     item.get('permission_expiry_date', self.default),
        #     item.get('temporary_permission_expiry_date', self.default),
        #     item.get('constraints', self.default)))
        #     self.conn.commit()
        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     return item

class Westminster(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'
        self.path = "/home/medi/workbench/boroughSpider/NAME_item_keys.pkl"

    @check_spider_pipeline
    def process_item(self, item, spider):
        store_keys(item, self.__class__.__name__)
        # try:
        #     self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
        #     address_response = self.cursor.fetchone()
        #     if address_response is None:
        #         self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
        #         self.conn.commit()
        #         self.cursor.execute("SELECT LAST_INSERT_ID();")
        #         address_response = self.cursor.fetchone()

        #     address_id = address_response[0]

        #     self.cursor.execute("""INSERT INTO boroughs
        #     (address_id,
        #     borough,
        #     domain,
        #     case_reference,
        #     ward,
        #     planning_case_officer,
        #     proposed_development,
        #     application_status,
        #     decision,
        #     appeal_decision,
        #     appeal_status,
        #     application_type,
        #     amenity_society,
        #     district_reference,
        #     applicants_name,
        #     contact_address,
        #     agent_name,
        #     agency_company_name,
        #     agent_address,
        #     environmental_assessment_requested,
        #     application_received_date,
        #     application_validated_date,
        #     documents_url,
        #     date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
        #     (address_id,
        #     item.get('borough', self.default),
        #     item.get('domain', self.default),
        #     item.get('reference', self.default),
        #     item.get('ward', self.default),
        #     item.get('case_officer', self.default),
        #     item.get('proposal', self.default),
        #     item.get('status', self.default),
        #     item.get('decision', self.default),
        #     item.get('appeal_decision', self.default),
        #     item.get('appeal_status', self.default),
        #     item.get('application_type', self.default),
        #     item.get('amenity_society', self.default),
        #     item.get('district_reference', self.default),
        #     item.get('applicant_name', self.default),
        #     item.get('applicant_address', self.default),
        #     item.get('case_officer', self.default),
        #     item.get('agent_company_name', self.default),
        #     item.get('agent_address', self.default),
        #     item.get('environmental_assessment_requested', self.default),
        #     item.get('application_received', self.default),
        #     item.get('application_validated', self.default),
        #     item.get('documents_url', self.default)))
        #     self.conn.commit()
        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     return item


class CityOfLondon(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'
        self.path = "/home/medi/workbench/boroughSpider/NAME_item_keys.pkl"

    @check_spider_pipeline
    def process_item(self, item, spider):
        print "From Pipeline -> Item: ", item
        store_keys(item, self.__class__.__name__)
        # try:
        #     self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
        #     address_response = self.cursor.fetchone()
        #     if address_response is None:
        #         self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
        #         self.conn.commit()
        #         self.cursor.execute("SELECT LAST_INSERT_ID();")
        #         address_response = self.cursor.fetchone()

        #     address_id = address_response[0]

        #     self.cursor.execute("""INSERT INTO boroughs
        #     (address_id,
        #     borough,
        #     domain,
        #     case_reference,
        #     ward,
        #     applicants_name,
        #     application_type,
        #     proposed_development,
        #     application_status,
        #     decision,
        #     decision_date,
        #     appeal_decision,
        #     appeal_decision_date,
        #     documents_url,
        #     planning_portal_reference,
        #     application_registration,
        #     application_validation,
        #     appeal_status,
        #     expected_decision_level,
        #     agent_name,
        #     agency_company_name,
        #     environmental_assessment_requested,
        #     closing_date_for_comments,
        #     statutory_expiry_date,
        #     agreed_expiry_date,
        #     permission_expiry_date,
        #     temporary_permission_expiry_date,
        #     constraints,
        #     date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());""",
        #     (address_id,
        #     item.get('borough', self.default),
        #     item.get('domain', self.default),
        #     item.get('reference', self.default),
        #     item.get('ward', self.default),
        #     item.get('applicant_name', self.default),
        #     item.get('application_type', self.default),
        #     item.get('proposal', self.default),
        #     item.get('status', self.default),
        #     item.get('decision', self.default),
        #     item.get('decision_date', self.default),
        #     item.get('appeal_decision', self.default),
        #     item.get('appeal_status', self.default),
        #     item.get('documents_url', self.default),
        #     item.get('planning_portal_reference', self.default),
        #     item.get('application_registered', self.default),
        #     item.get('application_validated', self.default),
        #     item.get('appeal_status', self.default),
        #     item.get('expected_decision_level', self.default),
        #     item.get('agent_name', self.default),
        #     item.get('agent_company_name', self.default),
        #     item.get('environmental_assessment_requested', self.default),
        #     item.get('closing_date_for_comments', self.default),
        #     item.get('statutory_expiry_date', self.default),
        #     item.get('agreed_expiry_date', self.default),
        #     item.get('permission_expiry_date', self.default),
        #     item.get('temporary_permission_expiry_date', self.default),
        #     item.get('constraints', self.default)))
        #     self.conn.commit()
        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     return item


class Wandsworth(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'
        self.path = "/home/medi/workbench/boroughSpider/NAME_item_keys.pkl"

    @check_spider_pipeline
    def process_item(self, item, spider):
        store_keys(item, self.__class__.__name__)
        # try:
        #     self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('site_address', self.default)])
        #     address_response = self.cursor.fetchone()
        #     if address_response is None:
        #         self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('site_address', self.default)])
        #         self.conn.commit()
        #         self.cursor.execute("SELECT LAST_INSERT_ID();")
        #         address_response = self.cursor.fetchone()
        #     address_id = address_response[0]

        #     self.cursor.execute("""INSERT INTO boroughs
        #     (address_id,
        #     borough,
        #     domain,
        #     case_reference,
        #     ward,
        #     applicants_name,
        #     application_type,
        #     proposed_development,
        #     application_status,
        #     decision,
        #     decision_date,
        #     appeal_decision,
        #     appeal_decision_date,
        #     documents_url,
        #     planning_portal_reference,
        #     application_registration,
        #     application_validation,
        #     appeal_status,
        #     expected_decision_level,
        #     agent_name,
        #     date_scraped) values (%s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, NOW());""",
        #     (address_id,
        #     item.get('borough', self.default),
        #     item.get('domain', self.default),
        #     item.get('application_number', self.default),
        #     item.get('wards', self.default),
        #     item.get('applicant', self.default),
        #     item.get('application_type', self.default),
        #     item.get('proposal', self.default),
        #     item.get('status', self.default),
        #     item.get('decision', self.default),
        #     item.get('decision_date', self.default),
        #     item.get('appeal_decision', self.default),
        #     item.get('appeal_submitted', self.default),
        #     item.get('documents_url', self.default),
        #     item.get('planning_portal_reference', self.default),
        #     item.get('application_registered', self.default),
        #     item.get('application_validated', self.default),
        #     item.get('appeal_lodged', self.default),
        #     item.get('expected_decision_level', self.default),
        #     item.get('case_officer_tel', self.default)))
        #     self.conn.commit()
        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     return item

class Hackney(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'
        self.path = "/home/medi/workbench/boroughSpider/NAME_item_keys.pkl"

    @check_spider_pipeline
    def process_item(self, item, spider):
        store_keys(item, self.__class__.__name__)
        # try:
        #     self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('site_address', self.default)])
        #     address_response = self.cursor.fetchone()
        #     if address_response is None:
        #         self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('site_address', self.default)])
        #         self.conn.commit()
        #         self.cursor.execute("SELECT LAST_INSERT_ID();")
        #         address_response = self.cursor.fetchone()
        #     address_id = address_response[0]

        #     self.cursor.execute("""INSERT INTO boroughs
        #     (address_id,
        #     borough,
        #     domain,
        #     case_reference,
        #     ward,
        #     applicants_name,
        #     application_type,
        #     proposed_development,
        #     application_status,
        #     decision,
        #     decision_date,
        #     appeal_decision,
        #     appeal_decision_date,
        #     documents_url,
        #     planning_portal_reference,
        #     application_registration,
        #     application_validation,
        #     appeal_status,
        #     expected_decision_level,
        #     agent_name,
        #     date_scraped) values (%s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, NOW());""",
        #     (address_id,
        #     item.get('borough', self.default),
        #     item.get('domain', self.default),
        #     item.get('application_number', self.default),
        #     item.get('wards', self.default),
        #     item.get('applicant', self.default),
        #     item.get('application_type', self.default),
        #     item.get('proposal', self.default),
        #     item.get('status', self.default),
        #     item.get('decision', self.default),
        #     item.get('decision_date', self.default),
        #     item.get('appeal_decision', self.default),
        #     item.get('appeal_submitted', self.default),
        #     item.get('documents_url', self.default),
        #     item.get('planning_portal_reference', self.default),
        #     item.get('application_registered', self.default),
        #     item.get('application_validated', self.default),
        #     item.get('appeal_lodged', self.default),
        #     item.get('expected_decision_level', self.default),
        #     item.get('case_officer_tel', self.default)))
        #     self.conn.commit()
        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     return item

class Southwark(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='192.168.1.207', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'
        self.path = "/home/medi/workbench/boroughSpider/NAME_item_keys.pkl"

    @check_spider_pipeline
    def process_item(self, item, spider):
        store_keys(item, self.__class__.__name__)
        # try:
        #     self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
        #     address_response = self.cursor.fetchone()
        #     if address_response is None:
        #         self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
        #         self.conn.commit()
        #         self.cursor.execute("SELECT LAST_INSERT_ID();")
        #         address_response = self.cursor.fetchone()
        #     address_id = address_response[0]

        #     self.cursor.execute("""INSERT INTO boroughs
        #     (address_id,
        #     borough,
        #     domain,
        #     case_reference,
        #     ward,
        #     applicants_name,
        #     application_type,
        #     proposed_development,
        #     application_status,
        #     decision,
        #     decision_date,
        #     appeal_decision,
        #     appeal_decision_date,
        #     documents_url,
        #     planning_portal_reference,
        #     planning_case_officer,
        #     application_registration,
        #     application_validation,
        #     appeal_status,
        #     expected_decision_level,
        #     agent_name,
        #     agency_company_name,
        #     environmental_assessment_requested,
        #     closing_date_for_comments,
        #     statutory_expiry_date,
        #     agreed_expiry_date,
        #     permission_expiry_date,
        #     temporary_permission_expiry_date,
        #     date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, %s, %s, %s, %s,
        #                                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());""",
        #     (address_id,
        #     item.get('borough', self.default),
        #     item.get('domain', self.default),
        #     item.get('reference', self.default),
        #     item.get('ward', self.default),
        #     item.get('applicant_name', self.default),
        #     item.get('application_type', self.default),
        #     item.get('proposal', self.default),
        #     item.get('status', self.default),
        #     item.get('decision', self.default),
        #     item.get('decision_date', self.default),
        #     item.get('appeal_decision', self.default),
        #     item.get('appeal_status', self.default),
        #     item.get('documents_url', self.default),
        #     item.get('planning_portal_reference', self.default),
        #     item.get('case_officer', self.default),
        #     item.get('application_registered', self.default),
        #     item.get('application_validated', self.default),
        #     item.get('appeal_status', self.default),
        #     item.get('expected_decision_level', self.default),
        #     item.get('agent_name', self.default),
        #     item.get('agent_company_name', self.default),
        #     item.get('environmental_assessment_requested', self.default),
        #     item.get('closing_date_for_comments', self.default),
        #     item.get('statutory_expiry_date', self.default),
        #     item.get('agreed_expiry_date', self.default),
        #     item.get('permission_expiry_date', self.default),
        #     item.get('expiry_date', self.default)))
        #     self.conn.commit()
        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     return item

class TowerHamlets(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'
        self.path = "/home/medi/workbench/boroughSpider/NAME_item_keys.pkl"

    @check_spider_pipeline
    def process_item(self, item, spider):
        store_keys(item, self.__class__.__name__)
        # try:
        #     self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('location', self.default)])
        #     address_response = self.cursor.fetchone()
        #     if address_response is None:
        #         self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('location', self.default)])
        #         self.conn.commit()
        #         self.cursor.execute("SELECT LAST_INSERT_ID();")
        #         address_response = self.cursor.fetchone()
        #     address_id = address_response[0]

        #     self.cursor.execute("""INSERT INTO boroughs
        #     (address_id,
        #     borough,
        #     domain,
        #     case_reference,
        #     ward,
        #     public_consultation_ends,
        #     applicants_name,
        #     application_type,
        #     proposed_development,
        #     decision,
        #     decision_date,
        #     documents_url,
        #     planning_portal_reference,
        #     application_registration,
        #     application_validation,
        #     agent_name,
        #     agency_company_name,
        #     date_scraped) values (%s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, NOW());""",
        #     (address_id,
        #     item.get('borough', self.default),
        #     item.get('domain', self.default),
        #     item.get('application_no', self.default),
        #     item.get('ward', self.default),
        #     item.get('consultation_end_date', self.default),
        #     item.get('applicant', self.default),
        #     item.get('application_type', self.default),
        #     item.get('development', self.default),
        #     item.get('decision_type', self.default),
        #     item.get('decision_date', self.default),
        #     item.get('documents_url', self.default),
        #     item.get('planning_portal_reference', self.default),
        #     item.get('application_registered', self.default),
        #     item.get('application_validated', self.default),
        #     item.get('case_officer', self.default),
        #     item.get('agent', self.default)))
        #     self.conn.commit()
        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     return item

class Islington(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'
        self.path = "/home/medi/workbench/boroughSpider/NAME_item_keys.pkl"

    @check_spider_pipeline
    def process_item(self, item, spider):
        store_keys(item, self.__class__.__name__)
        # try:
        #     self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('site_address', self.default)])
        #     address_response = self.cursor.fetchone()
        #     if address_response is None:
        #         self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('site_address', self.default)])
        #         self.conn.commit()
        #         self.cursor.execute("SELECT LAST_INSERT_ID();")
        #         address_response = self.cursor.fetchone()
        #     address_id = address_response[0]

        #     self.cursor.execute("""INSERT INTO boroughs
        #     (address_id,
        #     borough,
        #     domain,
        #     case_reference,
        #     ward,
        #     applicants_name,
        #     application_type,
        #     proposed_development,
        #     application_status,
        #     decision,
        #     decision_date,
        #     appeal_received,
        #     appeal_decision,
        #     appeal_decision_date,
        #     documents_url,
        #     planning_portal_reference,
        #     application_registration,
        #     application_validation,
        #     appeal_status,
        #     expected_decision_level,
        #     agent_name,
        #     date_scraped) values (%s, %s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, %s, %s, %s, %s,
        #                             %s, %s, %s, %s, NOW());""",
        #     (address_id,
        #     item.get('borough', self.default),
        #     item.get('domain', self.default),
        #     item.get('application_number', self.default),
        #     item.get('wards', self.default),
        #     item.get('applicant', self.default),
        #     item.get('application_type', self.default),
        #     item.get('proposal', self.default),
        #     item.get('status', self.default),
        #     item.get('decision', self.default),
        #     item.get('decision_date', self.default),
        #     item.get('appeal_submitted', self.default),
        #     item.get('appeal_decision', self.default),
        #     item.get('appeal_submitted', self.default),
        #     item.get('documents_url', self.default),
        #     item.get('planning_portal_reference', self.default),
        #     item.get('application_registered', self.default),
        #     item.get('application_validated', self.default),
        #     item.get('appeal_lodged', self.default),
        #     item.get('expected_decision_level', self.default),
        #     item.get('case_officer_tel', self.default)))
        #     self.conn.commit()
        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     return item

class GenericPipeline(object):
    def __init__(self):
      self.con = MySQLdb.connect(user='mehdi', passwd='pashmak.mN2', db='research_uk_public_data', host='granweb01', charset='utf8', use_unicode=True)
      self.cur = self.con.cursor()
      self.db_columns = []

    def table_exists(self, table_name):
      """"""
      query = "SHOW TABLES LIKE 'MYTABLE'".replace('MYTABLE', table_name)
      self.cur.execute(query)
      tables = self.cur.fetchone()
      exists = True if tables else False
      return exists

    def create_table(self, item, name):
      """"""
      column_types = []
      for it in item.keys():
        column_types.append((it, 'TEXT'))
      columns = ',\n'.join('`%s` %s' % x for x in column_types)
      create = """CREATE TABLE %(table_name)s (id INT(10) PRIMARY KEY AUTO_INCREMENT NOT NULL, %(columns)s, birth DATETIME NOT NULL, time_stamp TIMESTAMP);"""
      schema = create % {'table_name':name, 'columns':columns}
      try:
        self.cur.execute(schema)
        self.con.commit()
      except MySQLdb.Error as err:
        print "Error %d: %s" % (err.args[0], err.args[1])

      return item.keys()

    def check_db(self, diff_list, name):
      """"""
      import os
      path = os.getcwd()
      with open(path+'/%s.txt' % (name), 'w') as f:
        f.write('\n'.join(diff_list))

      column_types = []
      for it in diff_list:
        column_types.append(('ADD COLUMN `%s`' % it, 'TEXT'))
      columns = ',\n'.join('%s %s' % x for x in column_types)
      alter_sql = "ALTER TABLE %(table)s %(columns)s AFTER %(column)s;" % {'table':name, 'columns':columns, 'column':self.db_columns[-1]}
      try:
        self.cur.execute(alter_sql)
        self.con.commit()
      except MySQLdb.Error as err:
        print "Error %d: %s" % (err.args[0], err.args[1])

      return self.db_columns + diff_list

    def insert(self, item, name):
      """"""
      sql = "SHOW COLUMNS FROM TABLE;".replace('TABLE', name)
      self.cur.execute(sql)
      cols = self.cur.fetchall()
      cols = [col[0].encode('utf-8') for col in cols[1:-2]]
      col_names = ','.join('`%s`' % col for col in cols)
      wildcards = ','.join(['%s'] * len(cols))
      insert_sql = "INSERT INTO %s (%s, birth) VALUES (%s,NOW());" % (name, col_names, wildcards)
      data = tuple([item.get(col, 'n/a') for col in cols])
      self.cur.execute(insert_sql, data)
      self.con.commit()

    @check_spider_pipeline
    def process_item(self, item, spider):
      """"""
      # print "Item: ", item
      spider_class = spider.__class__.__name__
      # self.db_columns = item.keys()
      if self.table_exists(spider_class) is False:
        self.db_columns = self.create_table(item, spider_class)
      item_keys = item.keys()
      diff = list(set(item_keys) - set(self.db_columns))
      if diff:
        self.db_columns = self.check_db(diff, spider_class)
      try:
        self.insert(item, spider_class)
      except Exception as err:
        print "ERROR Froom process_item: ", err

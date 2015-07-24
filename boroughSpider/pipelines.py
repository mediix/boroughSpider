# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy import log
import MySQLdb
import functools
import sys
import re

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
            self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
            address_response = self.cursor.fetchone()
            if address_response is None:
                self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
                self.conn.commit()
                self.cursor.execute("SELECT LAST_INSERT_ID();")
                address_response = self.cursor.fetchone()

            address_id = address_response[0]

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
            date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, NOW())""",
            (address_id,
            item.get('borough', self.default),
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
            self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
            address_response = self.cursor.fetchone()
            if address_response is None:
                self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
                self.conn.commit()
                self.cursor.execute("SELECT LAST_INSERT_ID();")
                address_response = self.cursor.fetchone()

            address_id = address_response[0]

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
            date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());""",
            (address_id,
            item.get('borough', self.default),
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
            self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
            address_response = self.cursor.fetchone()
            if address_response is None:
                self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
                self.conn.commit()
                self.cursor.execute("SELECT LAST_INSERT_ID();")
                address_response = self.cursor.fetchone()

            address_id = address_response[0]

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
            date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (address_id,
            item.get('borough', self.default),
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
            self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
            address_response = self.cursor.fetchone()
            if address_response is None:
                self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
                self.conn.commit()
                self.cursor.execute("SELECT LAST_INSERT_ID();")
                address_response = self.cursor.fetchone()

            address_id = address_response[0]

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
            date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());""",
            (address_id,
            item.get('borough', self.default),
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


class Wandsworth(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('site_address', self.default)])
            address_response = self.cursor.fetchone()
            if address_response is None:
                self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('site_address', self.default)])
                self.conn.commit()
                self.cursor.execute("SELECT LAST_INSERT_ID();")
                address_response = self.cursor.fetchone()
            address_id = address_response[0]

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
            date_scraped) values (%s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, NOW());""",
            (address_id,
            item.get('borough', self.default),
            item.get('domain', self.default),
            item.get('application_number', self.default),
            item.get('wards', self.default),
            item.get('applicant', self.default),
            item.get('application_type', self.default),
            item.get('proposal', self.default),
            item.get('status', self.default),
            item.get('decision', self.default),
            item.get('decision_date', self.default),
            item.get('appeal_decision', self.default),
            item.get('appeal_submitted', self.default),
            item.get('documents_url', self.default),
            item.get('planning_portal_reference', self.default),
            item.get('application_registered', self.default),
            item.get('application_validated', self.default),
            item.get('appeal_lodged', self.default),
            item.get('expected_decision_level', self.default),
            item.get('case_officer_tel', self.default)))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item

class Hackney(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('site_address', self.default)])
            address_response = self.cursor.fetchone()
            if address_response is None:
                self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('site_address', self.default)])
                self.conn.commit()
                self.cursor.execute("SELECT LAST_INSERT_ID();")
                address_response = self.cursor.fetchone()
            address_id = address_response[0]

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
            date_scraped) values (%s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, NOW());""",
            (address_id,
            item.get('borough', self.default),
            item.get('domain', self.default),
            item.get('application_number', self.default),
            item.get('wards', self.default),
            item.get('applicant', self.default),
            item.get('application_type', self.default),
            item.get('proposal', self.default),
            item.get('status', self.default),
            item.get('decision', self.default),
            item.get('decision_date', self.default),
            item.get('appeal_decision', self.default),
            item.get('appeal_submitted', self.default),
            item.get('documents_url', self.default),
            item.get('planning_portal_reference', self.default),
            item.get('application_registered', self.default),
            item.get('application_validated', self.default),
            item.get('appeal_lodged', self.default),
            item.get('expected_decision_level', self.default),
            item.get('case_officer_tel', self.default)))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item

class Southwark(object):

    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='192.168.1.207', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('address', self.default)])
            address_response = self.cursor.fetchone()
            if address_response is None:
                self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('address', self.default)])
                self.conn.commit()
                self.cursor.execute("SELECT LAST_INSERT_ID();")
                address_response = self.cursor.fetchone()
            address_id = address_response[0]

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
            planning_case_officer,
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
            date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());""",
            (address_id,
            item.get('borough', self.default),
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
            item.get('case_officer', self.default),
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
            item.get('expiry_date', self.default)))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item

class TowerHamlets(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.default = 'n/a'

    @check_spider_pipeline
    def process_item(self, item, spider):
        try:
            self.cursor.execute("""SELECT a.id FROM addresses a WHERE a.address = %s;""", [item.get('location', self.default)])
            address_response = self.cursor.fetchone()
            if address_response is None:
                self.cursor.execute("""INSERT INTO addresses (address) VALUES (%s);""", [item.get('location', self.default)])
                self.conn.commit()
                self.cursor.execute("SELECT LAST_INSERT_ID();")
                address_response = self.cursor.fetchone()
            address_id = address_response[0]

            self.cursor.execute("""INSERT INTO boroughs
            (address_id,
            borough,
            domain,
            case_reference,
            ward,
            public_consultation_ends,
            applicants_name,
            application_type,
            proposed_development,
            decision,
            decision_date,
            documents_url,
            planning_portal_reference,
            application_registration,
            application_validation,
            agent_name,
            agency_company_name,
            date_scraped) values (%s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, NOW());""",
            (address_id,
            item.get('borough', self.default),
            item.get('domain', self.default),
            item.get('application_no', self.default),
            item.get('ward', self.default),
            item.get('consultation_end_date', self.default),
            item.get('applicant', self.default),
            item.get('application_type', self.default),
            item.get('development', self.default),
            item.get('decision_type', self.default),
            item.get('decision_date', self.default),
            item.get('documents_url', self.default),
            item.get('planning_portal_reference', self.default),
            item.get('application_registered', self.default),
            item.get('application_validated', self.default),
            item.get('case_officer', self.default),
            item.get('agent', self.default)))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item


# class Generic(object):
#     def __init__(self):
#         self.con = MySQLdb.connect(user='mehdi', passwd='pashmak.mN2', db='research_uk_public_data', host='granweb01', charset="utf8", use_unicode=True)
#         self.cur = self.con.cursor()
#         self.default = 'n/a'

#     def table_exists(self, name):
#         '''
#         colnames: column names for table to create
#         name: table's name
#         return: True if table schema exists, False otherwise
#         '''
#         import pandas.io.sql as psql
#         df = psql.read_sql("SHOW TABLES LIKE 'MYTABLE'".replace('MYTABLE', name), self.con)
#         exists = False if df.empty else True
#         return exists

#     def create_table(self, item, name):
#         '''
#         item: to create column names with item.keys
#         name: name of the table
#         return: create table in order for item insertion
#         '''
#         db_colname = lambda col: col.replace('_/_', '_').replace('?', '').strip()
#         column_types = []
#         for it in item.keys():
#             column_types.append((db_colname(it), 'TEXT'))

#         columns = ',\n'.join('%s %s' % x for x in column_types)
#         template_create = "CREATE TABLE %(name)s (%(columns)s);"
#         create = template_create % {'name': name, 'columns': columns}
#         return create

#     @check_spider_pipeline
#     def process_item(self, item, spider):
#         if self.table_exists(self.__class__.__name__):
#             print "exec if..."
#             print "Appending..."
#             wildcards = ','.join(['%s'] * len(item.values()))
#             cols = [ k for k in item.keys() ]
#             colnames = ','.join(cols)
#             insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, colnames, wildcards)
#             data = tuple(v if v else 'null' for v in item.values())
#             self.cur.execute(insert_sql, data)
#             self.con.commit()
#         else:
#             print "exec else..."
#             table = self.create_table(item, self.__class__.__name__)
#             self.cur.execute("SET sql_mode='ANSI_QUOTES';")
#             self.con.commit()
#             print 'schema\n', table
#             self.cur.execute(table)
#             self.con.commit()

#             wildcards = ','.join(['%s'] * len(item.values()))
#             cols = [ k for k in item.keys() ]
#             colnames = ','.join(cols)
#             insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, colnames, wildcards)
#             data = tuple(v for v in item.values())
#             self.cur.execute(insert_sql, data)
#             self.con.commit()


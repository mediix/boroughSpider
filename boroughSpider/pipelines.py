# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb, sys

class BoroughspiderPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect(user='scraper', passwd='12345678', db='granville', host='granweb01', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        try:
            self.cursor.execute("""INSERT INTO research_uk_boroughs
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
             date_scraped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
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
                                 item['documents_url'].encode('utf-8')))
            self.conn.commit()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return item

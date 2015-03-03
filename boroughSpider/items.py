# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from Scrapy.item import Item, Field


class ApplicationItem(Item):
    # define the fields for your item here like:
    # define the fields for your item here like:
    borough = Field()
    domain = Field()
    case_reference = Field()
    address = Field()
    ward = Field()
    polling_district = Field()
    listed_building_grade = Field()
    conservation_area = Field()
    applicants_name = Field()
    contact_name = Field()
    contact_address = Field()
    contact_telephone = Field()
    application_type = Field()
    proposed_development = Field()
    date_received = Field()
    registration_date = Field()
    public_consultation_ends = Field()
    application_status = Field()
    target_date_for_decision = Field()
    decision = Field()
    decision_date = Field()
    conditions_and_reasons = Field()
    formal_reference_number = Field()
    appeal_received = Field()
    appeal_start_date = Field()
    appeal_decision = Field()
    appeal_decision_date = Field()
    planning_case_officer = Field()
    planning_team = Field()
    documents_url = Field()
    pass

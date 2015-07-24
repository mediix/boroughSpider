# -*- coding: utf-8 -*-

# Scrapy settings for boroughSpider project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
BOT_NAME = 'boroughSpider'

SPIDER_MODULES = ['boroughSpider.spiders']
NEWSPIDER_MODULE = 'boroughSpider.spiders'

# DOWNLOAD_DELAY = 0.25

COOKIES_ENABLED = True
COOKIES_DEBUG = True

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'itempider (+http://www.yourdomain.com)'

ITEM_PIPELINES = {
    'boroughSpider.pipelines.Westminster',
    'boroughSpider.pipelines.Hammersmith',
    'boroughSpider.pipelines.Kensington',
    'boroughSpider.pipelines.CityOfLondon',
    'boroughSpider.pipelines.Wandsworth',
    'boroughSpider.pipelines.Southwark',
    'boroughSpider.pipelines.TowerHamlets'
}

USER_AGENT = "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.132 Safari/537.36"

# DOWNLOADER_MIDDLEWARES = {
#     'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': 110,
#     'boroughSpider.middlewares.ProxyMiddleware': 100,
# }

##############################################################################
'''
EXPORT_FIELDS = [
    'borough',
    'domain',
    'case_reference',
    'address',
    'ward',
    'polling_district',
    'listed_building_grade',
    'conservation_area',
    'applicants_name',
    'contact_name',
    'contact_address',
    'contact_telephone',
    'application_type',
    'proposed_development',
    'date_received',
    'registration_date',
    'public_consultation_ends',
    'application_status',
    'target_date_for_decision',
    'decision',
    'decision_date',
    'conditions_and_reasons',
    'formal_reference_number',
    'appeal_received',
    'appeal_start_date',
    'appeal_decision',
    'appeal_decision_date',
    'planning_case_officer',
    'planning_team'
]
FEED_URI = 'report.csv'
FEED_FORMAT = 'csv'

FEED_EXPORTERS = {
    'csv': 'boroughSpider.feedexport.CSVkwItemExporter'
}
'''

from scrapy.spiders import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest

from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser

class towerHamletsSpider(Spider):
  name = 'towerSpider'
  pipeline = ['TowerHamlets']
  domain = 'http://www.towerhamlets.gov.uk'
  base_url = ["http://planreg.towerhamlets.gov.uk"]
  start_urls = ["http://planreg.towerhamlets.gov.uk/WAM/monthlyDecisions.do?action=init"]

  def parse(self, response):
    # inspect_response(response)
    return [FormRequest.from_response(response,
                          formname='searchCriteriaFileTypeForm',
                          formdata={'action': 'showMonthlyList',
                                    'areaCode': "%",
                                    'sortOrder': '3',
                                    'endDate': '0',
                                    'decisionType': "%",
                                    'Button': 'Search'},
                          callback=self.parse_results)]

  def parse_results(self, response):
    # inspect_response(response, self)
    urls = response.xpath("//table[@id='searchresults']//tr//td//a/@href").extract()

    for url in urls:
      app_url = '{0}{1}'.format(self.base_url[0], url.encode('utf-8'))
      yield FormRequest(app_url, method="GET", callback = self.parse_item)

  def parse_item(self, response):
    # inspect_response(response, self)
    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = list(prototypes.convert_table(tab.xpath("//table")))[0]
    table = { key.replace(':', '').replace(' ', '_').lower(): value[0].encode('utf-8') \
      if value else '' for key, value in table.items() }

    for key, value in table.items():
      try:
        if value == '':
          table[key] = 'n/a'
        elif value.isdigit():
          table[key] = value
        else:
          table[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
      except:
        table[key] = value

    table.update({'borough': 'TowerHamlets'})
    table.update({'domain': self.domain})
    table.update({'documents_url': response.url})

    return table

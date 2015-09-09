from scrapy.spiders import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field
#
from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser

class CamdenSpider(Spider):
  name = 'camdSpider'
  domain = 'http://www.camden.gov.uk'
  pipeline = ['Hackney']
  base_url = ["http://planningrecords.camden.gov.uk/Northgate/PlanningExplorer17/Generic/"]
  start_urls = ["http://planningrecords.camden.gov.uk/Northgate/PlanningExplorer17/GeneralSearch.aspx"]

  custom_settings = {
      'DOWNLOAD_DELAY': 0.25,
      'RETRY_ENABLED': True,
      'CONCURRENT_REQUESTS': 1,
      'CONCURRNT_REQUESTS_PER_IP': 1,
      'RANDOM_DOWNLOAD_DELY': False,
      'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
      'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
  }

  def parse(self, response):
    return [FormRequest.from_response(response,
                                      formname = 'Template',
                                      formdata = { 'cboSelectDateValue':'DATE_RECEIVED',
                                                   'rbGroup':'rbMonth',
                                                   'cboMonths':'12' },
                                      callback = self.parse_search_result)]

  def parse_search_result(self, response):
    # inspect_response(response, self)
    delete = ""
    i = 1
    while (i < 0x20):
      delete += chr(i)
      i+=1

    # while (True):
    if response.xpath("//div[@class='align_center']/a[preceding::span[@class='results_page_number_sel'] and \
      not(@class='noborder')]/@href").extract():
      app_urls = response.xpath("//td[@title='View Application Details']//a/@href").extract()
      app_urls = [url.encode('utf-8').translate(None, delete) for url in app_urls]
      for url in app_urls:
        application_url = '{0}{1}'.format(self.base_url[0], url.encode('utf-8'))
        yield FormRequest(application_url, method="GET", callback = self.parse_applications)
      try:
        next_page =  response.xpath("//div[@class='align_center']/a[preceding::span[@class='results_page_number_sel'] and not(@class='noborder')]/@href").extract()[0]
        next_page = next_page.encode('utf-8').translate(None, delete)
        next_page_url = '{0}{1}'.format(self.base_url[0], next_page)
        yield FormRequest(next_page_url, method="GET", callback = self.parse_search_result)
      except:
        pass
    else:
      app_urls = response.xpath("//td[@title='View Application Details']//a/@href").extract()
      app_urls = [url.encode('utf-8').translate(None, delete) for url in app_urls]
      for url in app_urls:
        application_url = '{0}{1}'.format(self.base_url[0], url.encode('utf-8'))
        yield FormRequest(application_url, method="GET", callback = self.parse_applications)

  def parse_applications(self, response):
    # inspect_response(response, self)
    delete = ""
    i = 1
    while (i < 0x20):
      delete += chr(i)
      i+=1

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = tab.xpath("//div[@class='dataview']//ul//li")
    table = [[str(text.strip().encode('utf-8')).strip() for text in elem.itertext()] for elem in table]
    table = [[x for x in elem if x != ''] for elem in table]

    table = { (t[0] if t else ''): (t[1:] if t else '') for t in table }
    table.pop('', None)
    chk = lambda key: key.replace(' ', '_').replace('_/_', '_').replace('?', '')
    table = { chk(key).lower(): (value[0] if value else '') for key, value in table.items() }

    table.update({'borough': 'Camden'})
    table.update({'domain': self.domain})
    try:
      documents_url = response.xpath("//*[@id='content']//a[@title='Link to View Related Documents']/@href").extract()[0]
    except:
      table.update({'documents_url': 'n/a'})
    else:
      table.update({'documents_url': documents_url.encode('utf-8').translate(None, delete)})

    if response.xpath("//*[@id='listing-content-rhs']//a[@title='Link to the application Dates page.']/@href").extract():
      date_url = response.xpath("//*[@id='listing-content-rhs']//a[@title='Link to the application Dates page.']/@href").extract()[0]
      date_url = '{0}{1}'.format(self.base_url[0], date_url.encode('utf-8').translate(None, delete))
      return [FormRequest(date_url, method="GET", meta={'table':table}, callback=self.parse_dates)]
    else:
      return table

  def parse_dates(self, response):
    # inspect_response(response, self)
    table = response.meta['table']

    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table_1 = tab.xpath("//div[@class='dataview']//ul//li")
      table_1 = [[str(text.strip().encode('utf-8')).strip() for text in elem.itertext()] for elem in table_1]
      table_1 = [[x for x in elem if x != ''] for elem in table_1]

      table_1 = { (t[0] if t else ''): (t[1:] if t else '') for t in table_1 }
      table_1.pop('', None)
      chk = lambda key: key.replace(' ', '_').replace('_/_', '_').replace('?', '')
      table_1 = { chk(key).lower(): (value[0] if value else '') for key, value in table_1.items() }
    except Exception as err:
      pass
    else:
      table.update(table_1)

    for key, value in table.items():
      try:
        if value == '':
          table[key] = ''
        elif value.isdigit():
          table[key] = value
        else:
          table[key] = parser.parse(value.encode('utf-8')).strftime("%Y-%m-%d")
      except Exception as err:
        table[key] = value

    return table

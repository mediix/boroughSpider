from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field
#
from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser

class HackneySpider(Spider):
  name = 'hackSpider'
  domain = 'http://www.hackney.gov.uk'
  # pipeline = 'Hackney'
  base_url = ["http://planning.hackney.gov.uk/Northgate/PlanningExplorer/Generic/"]
  start_urls = ["http://planning.hackney.gov.uk/Northgate/PlanningExplorer/generalsearch.aspx"]

  def create_item_class(self, class_name, field_list):
    fields = {}
    for field_name in field_list:
      fields[field_name] = Field()

    fields.update({'domain': Field()})
    fields.update({'borough': Field()})
    fields.update({'documents_url': Field()})
    return type(class_name, (DictItem,), {'fields':fields})

  def parse(self, response):
    return [FormRequest.from_response(response,
                                      formname = 'M3Form',
                                      formdata = { 'cboSelectDateValue':'DATE_RECEIVED',
                                                   'rbGroup':'rbMonth',
                                                   'cboMonths':'1' },
                                      callback = self.parse_search_result)]

  def parse_search_result(self, response):
    # inspect_response(response, self)
    #
    delete = ""
    i = 1
    while (i < 0x20):
      delete += chr(i)
      i+=1

    if response.xpath("//div[@class='align_center']/a[preceding::span[@class='results_page_number_sel'] and \
      not(@class='noborder')]/@href").extract():
      app_urls = response.xpath("//td[@title='View Application Details']//a/@href").extract()
      app_urls = [str(url).translate(None, delete) for url in app_urls]
      for url in app_urls:
        application_url = '{0}{1}'.format(self.base_url[0], str(url))
        yield FormRequest(application_url, method="GET", callback = self.parse_applications)
      try:
        next_page =  response.xpath("//div[@class='align_center']/a[preceding::span[@class='results_page_number_sel'] and not(@class='noborder')]/@href").extract()[0]
        next_page = str(next_page).translate(None, delete)
        next_page_url = '{0}{1}'.format(self.base_url[0], next_page)
        yield FormRequest(next_page_url, method="GET", callback = self.parse_search_result)
      except:
        pass
    else:
      app_urls = response.xpath("//td[@title='View Application Details']//a/@href").extract()
      app_urls = [str(url).translate(None, delete) for url in app_urls]
      for url in app_urls:
        application_url = '{0}{1}'.format(self.base_url[0], str(url))
        yield FormRequest(application_url, method="GET", callback = self.parse_applications)

  def parse_applications(self, response):
    # inspect_response(response)
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

    if response.xpath("//*[text()='Application Constraints']").extract() or \
        response.xpath("//*[text()='Application Dates']").extract():
      try:
        const_url = self.base_url[0] + \
          response.xpath("//*[text()='Application Constraints']/@href").extract()[0].encode('utf-8', 'ignore')
        dates_url = self.base_url[0] + \
          response.xpath("//*[text()='Application Dates']/@href").extract()[0].encode('utf-8', 'ignore')
        return FormRequest(dates_url, method="GET", meta={'url': [const_url, dates_url], 'table':table}, callback = self.parse_dates)
      except:
        return FormRequest(const_url, method="GET", meta={'table':table}, callback=self.parse_constraints)
    else:
      hackneyItem = self.create_item_class('hackneyItem', table.keys())
      item = hackneyItem()

      for key, value in table.items():
        try:
          item[key] = value
        except:
          item[key] = 'n/a'

      item['borough'] = "Hackney"
      item['domain'] = self.domain
      try:
        documents_url = response.xpath("//*[@title='Link to documents']/@href").extract()[0]
        item['documents_url'] = str(documents_url)
      except:
        item['documents_url'] = 'n/a'

      return item

  def parse_dates(self, response):
    # inspect_response(response, self)
    #
    const_url = response.meta['url']
    table0 = response.meta['table']

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = tab.xpath("//div[@class='dataview']//ul//li")
    table = [[str(text.strip().encode('utf-8')).strip() for text in elem.itertext()] for elem in table]
    table = [[x for x in elem if x != ''] for elem in table]

    table = { (t[0] if t else ''): (t[1:] if t else '') for t in table }
    table.pop('', None)
    chk = lambda key: key.replace(' ', '_').replace('_/_', '_').replace('?', '')
    table = { chk(key).lower(): (value[0] if value else '') for key, value in table.items() }
    table.update(table0)

    return FormRequest(const_url, method="GET", meta={'table':table}, callback = self.parse_constraints)

  def parse_constraints(self, response):
    # inspect_response(response, self)
    #
    table0 = response.meta['table']

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = tab.xpath("//div[@class='dataview']//ul//li")
    table = [[str(text.strip().encode('utf-8')).strip() for text in elem.itertext()] for elem in table]
    table = [[x for x in elem if x != ''] for elem in table]

    table = { (t[0] if t else ''): (t[1:] if t else '') for t in table }
    table.pop('', None)
    chk = lambda key: key.replace(' ', '_').replace('_/_', '_').replace('?', '')
    table = { chk(key).lower(): (value[0] if value else '') for key, value in table.items() }
    table.update(table0)

    import pdb; pdb.set_trace()

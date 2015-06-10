from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field
#
from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser
#
from datetime import date, datetime, timedelta
import time, json

class LambethSpider(Spider):
  name = 'lambSpider'

  domain = 'lambeth.gov.uk'

  pipeline = 'Hammersmith'

  base_url = ["http://planning.lambeth.gov.uk"]

  start_urls = ["http://planning.lambeth.gov.uk/online-applications/search.do?action=monthlyList"]

  def create_item_class(self, class_name, field_list):
    fields = {}
    for field_name in field_list:
      fields[field_name] = Field()

    fields.update({'domain': Field()})
    fields.update({'borough': Field()})
    fields.update({'constraints': Field()})
    fields.update({'documents_url': Field()})
    return type(class_name, (DictItem,), {'fields':fields})

  def parse(self, response):
  	# inspect_response(response)
  	for month in response.xpath("//*[@id='month']/option/text()").extract():
  		yield FormRequest.from_response(response,
                        formname = 'searchCriteriaForm',
                        formdata = { 'month':str(month),
                                     'dateType': 'DC_Decided',
                                     'searchType':'Application' },
                        callback = self.parse_results)

  def parse_results(self, response):
  	# inspect_response(response)
  	try:
  		yield FormRequest.from_response(response,
                          formname = 'searchCriteriaForm',
                          formdata = { 'searchCriteria.page':'1',
                                       'action':'page',
                                       'orderBy':'DateReceived',
                                       'orderByDirection':'Descending',
                                       'searchCriteria.resultsPerPage':'100' },
                          callback = self.parse_long_results)
  	except:
  		yield FormRequest(response.url, method="GET", callback = self.parse_long_results)
  	finally:
  		pass

  def parse_long_results(self, response):
  	# inspect_response(response)

  	if response.xpath("//*[@class='pager top']/span[@class='showing']"):
  		for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
  			item_url = '{0}{1}'.format(self.base_url[0], 	str(url))
  			yield FormRequest(item_url, method="GET", callback = self.parse_summary)

     	for href in response.xpath("//p[@class='pager top']/a[@class='page']/@href").extract():
     		nxt_url = '{0}{1}'.format(self.base_url[0], str(href))
     		yield FormRequest(nxt_url, method="GET", callback = self.parse_items)
  	else:
  		try:
  			for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
  				item_url = '{0}{1}'.format(self.base_url[0], str(url))
  				yield FormRequest(item_url, method="GET", callback = self.parse_items)
  		except:
  			pass

  def parse_items(self, response):
  	# inspect_response(response)

    for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[0], str(url))
      yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_summary(self, response):
    # inspect_response(response)

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table = list(prototypes.convert_table(tab.xpath("//table")))[0]

    further_info_url = response.xpath("//*[@id='subtab_details']/@href").extract()[0]
    further_info_url = '{0}{1}'.format(self.base_url[0], str(further_info_url))
    request = FormRequest(further_info_url, method = "GET",
                          meta = {'table':table},
                          callback = self.parse_further_info)
    return request

  def parse_further_info(self, response):
    # inspect_response(response)

    table = response.meta['table']

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table.update(list(prototypes.convert_table(tab.xpath("//table")))[0])

    important_dates_url = response.xpath("//*[@id='subtab_dates']/@href").extract()[0]
    important_dates_url = '{0}{1}'.format(self.base_url[0], str(important_dates_url))
    request = FormRequest(important_dates_url, method = "GET",
                          meta = {'table':table},
                          callback = self.parse_important_dates)
    return request

  def parse_important_dates(self, response):
    #inspect_response(response)

    table = response.meta['table']

    strat = (parse_html,)

    tab = extract(response.body, strategy=strat)
    table.update(list(prototypes.convert_table(tab.xpath("//table")))[0])

    if response.xpath("//*[@id='tab_constraints']/@href").extract():
      constraint_url = response.xpath("//*[@id='tab_constraints']/@href").extract()[0]
      constraint_url = '{0}{1}'.format(self.base_url[0], str(constraint_url))
      request = FormRequest(constraint_url, method = "GET",
                            meta = {'table':table},
                            callback = self.parse_constraints)
      return request
    else:
      table = {key.replace(' ', '_').lower(): value[0].encode('utf-8') for key, value in table.items()}

      lambethItem = self.create_item_class('lambethItem', table.keys())

      item = lambethItem()

      for key, value in table.items():
        try:
          if value == '':
            item[key] = 'n/a'
          elif value.isdigit():
            item[key] = value
          else:
            item[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
        except:
          item[key] = value

      item['borough'] = "Lambeth"
      item['domain'] = self.domain
      try:
        documents_url = response.xpath("//*[@id='tab_externalDocuments']/@href").extract()[0]
        documents_url = '{0}{1}'.format(self.base_url[0], str(documents_url))
        item['documents_url'] = documents_url
      except:
        item['documents_url'] = "n/a"
      return item

  def parse_constraints(self, response):
    # inspect_response(response)

    table = response.meta['table']

    td = []
    td += response.xpath("//table[@id='caseConstraints']/tr/td/text()").extract()

    doc_dict = {}
    while td:
      doc_dict[str(td.pop())] = str(td)

    json_dict = json.dumps(doc_dict, ensure_ascii=False)

    table = {key.replace(' ', '_').lower(): value[0].encode('utf-8') for key, value in table.items()}

    lambethItem = self.create_item_class('lambethItem', table.keys())

    item = lambethItem()

    for key, value in table.items():
      try:
        if value == '':
          item[key] = 'n/a'
        elif value.isdigit():
          item[key] = value
        else:
          item[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
      except:
        item[key] = value

    item['constraints'] = json_dict
    item['borough'] = "Lambeth"
    item['domain'] = self.domain
    try:
      documents_url = response.xpath("//*[@id='tab_externalDocuments']/@href").extract()[0]
      documents_url = '{0}{1}'.format(self.base_url[0], str(documents_url))
      item['documents_url'] = documents_url
    except:
      item['documents_url'] = 'n/a'

    return item

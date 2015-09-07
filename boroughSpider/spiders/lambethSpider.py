from scrapy.spiders import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
#
from libextract import extract, prototypes
from libextract.tabular import parse_html
from dateutil import parser
#
from datetime import date, datetime, timedelta
import json

class LambethSpider(Spider):
  name = 'lambSpider'
  domain = 'http://planning.lambeth.gov.uk'
  pipeline = ['Lambeth']
  base_url = ["http://planning.lambeth.gov.uk"]
  start_urls = ["http://planning.lambeth.gov.uk/online-applications/search.do?action=monthlyList"]

  def __init__(self, month, **kwargs):
    self.month = month

  def parse(self, response):
  	# inspect_response(response)
  	return [FormRequest.from_response(response,
                        formname = 'searchCriteriaForm',
                        formdata = { 'month':self.month,
                                     'dateType': 'DC_Decided',
                                     'searchType':'Application' },
                        callback = self.parse_results)]

  def parse_results(self, response):
  	# inspect_response(response)
  	return [FormRequest.from_response(response,
                          formname = 'searchCriteriaForm',
                          formdata = { 'searchCriteria.page':'1',
                                       'action':'page',
                                       'orderBy':'DateReceived',
                                       'orderByDirection':'Descending',
                                       'searchCriteria.resultsPerPage':'100' },
                          callback = self.parse_long_results)]

  def parse_long_results(self, response):
  	# inspect_response(response)

  	if response.xpath("//*[@class='pager top']/span[@class='showing']"):
  		for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
  			item_url = '{0}{1}'.format(self.base_url[0], 	url.encode('utf-8'))
  			yield FormRequest(item_url, method="GET", callback = self.parse_summary)

     	for href in response.xpath("//p[@class='pager top']/a[@class='page']/@href").extract():
     		nxt_url = '{0}{1}'.format(self.base_url[0], href.encode('utf-8'))
     		yield FormRequest(nxt_url, method="GET", callback = self.parse_items)
  	else:
  		try:
  			for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
  				item_url = '{0}{1}'.format(self.base_url[0], url.encode('utf-8'))
  				yield FormRequest(item_url, method="GET", callback = self.parse_summary)
  		except:
  			pass

  def parse_items(self, response):
  	# inspect_response(response)
    for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[0], url.encode('utf-8'))
      yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_summary(self, response):
    # inspect_response(response)
    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except Exception:
      raise IndexError('Table Not Found!')

    if response.xpath("//*[@id='subtab_details']/@href").extract():
      further_info_url = response.xpath("//*[@id='subtab_details']/@href").extract()[0]
      further_info_url = '{0}{1}'.format(self.base_url[0], further_info_url.encode('utf-8'))
      return [FormRequest(further_info_url, method = "GET",
                                            meta = {'table':table},
                                            callback = self.parse_further_info)]
    else:
      pass

  def parse_further_info(self, response):
    # inspect_response(response)
    table = response.meta['table']

    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table_1 = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except Exception:
      raise IndexError('Table Not Found!')
    else:
      table.update(table_1)

    if response.xpath("//*[@id='subtab_dates']").extract():
      url = response.xpath("//*[@id='subtab_dates']/@href").extract()[0]
      important_dates = '{0}{1}'.format(self.base_url[0], url.encode('utf-8'))
      return FormRequest(important_dates, method="GET", meta={'table':table}, callback=self.parse_important_dates)

    else:
      table = { k.replace(' ', '_').lower(): v[0].encode('utf-8') for k, v in table.items() }

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

      table.update({'borough': "Lambeth"})
      table.update({'domain': self.domain})
      try:
        documents_url = response.xpath("//*[@id='tab_externalDocuments']/@href").extract()[0]
        documents_url = '{0}{1}'.format(self.base_url[0], documents_url)
        table.update({'documents_url': documents_url})
      except Exception:
        table.update({'documents_url': 'n/a'})

      return table

  def parse_important_dates(self, response):
    #inspect_response(response)
    table = response.meta['table']

    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table_1 = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except Exception:
      raise IndexError('Table Not Found!')
    else:
      table.update(table_1)

    if response.xpath("//*[@id='tab_constraints']/@href").extract():
      constraint_url = response.xpath("//*[@id='tab_constraints']/@href").extract()[0]
      constraint_url = '{0}{1}'.format(self.base_url[0], constraint_url.encode('utf-8'))
      return [FormRequest(constraint_url, method = "GET",
                                          meta = {'table':table},
                                          callback = self.parse_constraints)]
    else:
      table = {key.replace(' ', '_').lower(): value[0].encode('utf-8') for key, value in table.items()}

      for key, value in table.items():
        try:
          if value == '':
            table[key] = ''
          elif value.isdigit():
            table[key] = value
          else:
            table[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
        except:
          table[key] = value

      table.update({'borough': "Lambeth"})
      table.update({'domain': self.domain})
      try:
        documents_url = response.xpath("//*[@id='tab_externalDocuments']/@href").extract()[0]
        documents_url = '{0}{1}'.format(self.base_url[0], documents_url)
        table.update({'documents_url': documents_url})
      except Exception:
        table.update({'documents_url': 'n/a'})

      return table

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
    table.update({'constraints': json_dict})

    for key, value in table.items():
      try:
        if value == '':
          table[key] = ''
        elif value.isdigit():
          table[key] = value
        else:
          table[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
      except:
        table[key] = value

    table.update({'borough': "Lambeth"})
    table.update({'domain': self.domain})
    try:
      documents_url = response.xpath("//*[@id='tab_externalDocuments']/@href").extract()[0]
      documents_url = '{0}{1}'.format(self.base_url[0], documents_url)
      table.update({'documents_url': documents_url})
    except Exception:
      table.update({'documents_url': 'n/a'})

    return table

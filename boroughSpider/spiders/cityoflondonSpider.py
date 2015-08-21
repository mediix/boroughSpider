from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest

from libextract import extract, prototypes
from libextract.tabular import parse_html
from datetime import date, datetime, timedelta
from dateutil import parser

class cityOfLondonSpider(Spider):
  name = 'londSpider'
  pipeline = ['CityOfLondon', 'GenericPipeline']
  domain = 'cityoflondon.gov.uk'
  base_url = ["dummy", "http://www.planning2.cityoflondon.gov.uk"]
  start_urls = ["http://www.planning2.cityoflondon.gov.uk/online-applications/search.do?action=monthlyList"]

  def __init__(self, month=None):
    self.month = month

  def parse(self, response):
    print "Post Request Month:", self.month
    return [FormRequest.from_response(response,
                        formname = 'searchCriteriaForm',
                        formdata = { 'searchCriteria.caseStatus':'',
                                     'searchCriteria.ward':'',
                                     'month':self.month,
                                     'dateType':'DC_Validated',
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
    # inspect_response(response, self)

    # if there exists pagination
    if response.xpath("//*[@class='pager top']/span[@class='showing']"):
      # parse current page items
      for href in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
        item_url = '{0}{1}'.format(self.base_url[1], href.encode('utf-8'))
        yield FormRequest(item_url, method="GET", callback = self.parse_summary)
      # parse the next page of items
      for url in response.xpath("//p[@class='pager top']/a[@class='page']/@href").extract():
        nxt_url = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
        yield FormRequest(nxt_url, method="GET", callback = self.parse_items)
    # if there is not pagination
    else:
      for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
        item_url = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
        yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_items(self, response):
    # inspect_response(response, self)
    for url in response.xpath("//*[@id='searchresults']//li/a/@href").extract():
      item_url = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
      yield FormRequest(item_url, method="GET", callback = self.parse_summary)

  def parse_summary(self, response):
    # inspect_response(response)
    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except IndexError as err:
      pass

    if response.xpath("//*[@id='subtab_details']/@href").extract():
      url = response.xpath("//*[@id='subtab_details']/@href").extract()[0]
      further_info = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
      return FormRequest(further_info, method = "GET", meta={'table':table}, callback=self.parse_further_info)
    else:
      pass

  def parse_further_info(self, response):
    # inspect_response(response)
    table = response.meta['table']

    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table_1 = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except IndexError as err:
      pass
    else:
      table.update(table_1)

    if response.xpath("//*[@id='subtab_dates']").extract():
      url = response.xpath("//*[@id='subtab_dates']/@href").extract()[0]
      important_dates = '{0}{1}'.format(self.base_url[1], url.encode('utf-8'))
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

      table.update({'borough': "City of London"})
      table.update({'domain': self.domain})

      try:
        documents_url = response.xpath("//*[@id='tab_documents']/@href").extract()[0]
        documents_url = '{0}{1}'.format(self.base_url[1], documents_url)
        table.update({'documents_url': documents_url})
      except Exception as err:
        table.update({'documents_url': 'n/a'})

      item = table
      return item

  def parse_important_dates(self, response):
    # inspect_response(response, self)
    table = response.meta['table']

    strat = (parse_html,)
    tab = extract(response.body, strategy=strat)
    try:
      table_1 = list(prototypes.convert_table(tab.xpath("//table")))[0]
    except IndexError as err:
      pass
    else:
      table.update(table_1)

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

    table.update({'borough': "City of London"})
    table.update({'domain': self.domain})

    try:
      documents_url = response.xpath("//*[@id='tab_documents']/@href").extract()[0]
    except Exception as err:
      table.update({'documents_url': 'n/a'})
    else:
      documents_url = '{0}{1}'.format(self.base_url[1], documents_url)
      table.update({'documents_url': documents_url})

    if documents_url:
      self.downloader(documents_url, response.headers['Set-Cookie'])

    item = table
    return item

  def downloader(self, url=None, cookie=None):
    """"""
    def select(data=None):
      """"""
      vals = []
      for key, value in data.items():
        for elem in value:
          for k, v in elem.items():
            if v == 'Application Form':
              vals.append(elem.get('view'))
      return (key, vals)

    try:
      import requests
      from bs4 import BeautifulSoup
      from boroughSpider.settings import files_storage
    except ImportError as err:
      print err

    print "FILE STORAGE: ", files_storage
    resp = requests.get(url, cookies=cookie)
    resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text)
    docs_table = soup.find('table', {'id': 'Documents'})  # City of Westminster
    #
    keys = []
    for th in docs_table.findAll('th'):
      keys.append(str(th.get_text(strip=True)).lower().replace(' ', '_'))

    data = []
    for tr in docs_table.findAll('tr')[1:]: # skip the table header
      vals = []
      for td in tr.findAll('td'):
        if td.find('a'):
          vals.append(self.base_url[1] + td.find('a').get('href'))
        else:
          vals.append(td.get_text(strip=True))
      #
      data.append(dict(zip(keys, vals)))

    data = select(data)
    key = data[0]
    item = data[1]
    file_name = '_application_form'
    ext = '.pdf'
    fk = lambda x: x.replace('/', '_')

    if len(item) == 0:
      print "NO file to download"
    elif len(item) > 1:
      for idx, it in enumerate(item):
        try:
          name = files_storage + fk(key) + file_name + '_' + str(idx+1) + ext
          # url = it
          # url_parts = url.split('?')
          # url_parts[1] = urlencode({'test':url_parts[1]})
          # url = '{0}?{1}'.format(url_parts[0], url_parts[1])
          response = requests.get(it, cookies=cookie)
          f = open(name, 'wb')
          f.write(response.content)
        except Exception, e:
          print "ERROR FROM download_resource: elif: ", e
        else:
          print "%s Download Completed" % (fk(key)+file_name+'_'+str(idx+1)+ext)
          f.close()
    else:
      try:
        print item
        response = requests.get(item[0])
        f = open(files_storage + fk(key) + file_name + ext, 'wb')
        f.write(response.content)
      except Exception as e:
        print "ERROR FROM download_source: else", e
      else:
        print "%s Download Completed" % (fk(key)+file_name+ext)
        f.close()



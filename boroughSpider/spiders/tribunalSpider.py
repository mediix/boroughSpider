from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field
from bs4 import BeautifulSoup

from libextract import extract, prototypes
from libextract.tabular import parse_html

class tribunalSpider(Spider):
	name = 'tribSpider'
	pipeline = 'Tribunal'
	base_url = 'http://www.lease-advice.org/lvtdecisions/'
	start_urls = ['http://www.lease-advice.org/lvtdecisions/tables.asp?table=3']

	def create_item_class(self, class_name, field_list):
		fields = {}
		for field_name in field_list:
			fields[field_name] = Field()
		return type(class_name, (DictItem,), {'fields':fields})

	def parse(self, response):
		links = response.xpath("//p[text()='View decision numbers: ']//@href").extract()

		# for link in links:
		url = '{0}{1}'.format(self.base_url, str(links[0]))
		yield FormRequest(url, method="GET", callback=self.parse_result)

	def parse_result(self, response):
		# inspect_response(response, self)

		soup = BeautifulSoup(response.body, 'html.parser')
		table = soup.find('table', {'class': 'lvtFullTable'})
		keys = []
		for th in table.findAll('tr')[1]:
			keys.append(str(th.get_text(strip=True)).replace(' ', '_'))

		tribunalItem = self.create_item_class('tribunalItem', keys)
		item = tribunalItem()

		items = []
		vals = []
		for tr in table.findAll('tr'):
			for td in tr.findAll('td'):
				if td.find('a'):
					vals.append('{0}{1}'.format(self.base_url, td.find('a').get('href').encode('utf-8').replace('../', '')))
			vals.append(td.get_text(strip=True).encode('ascii', 'ignore'))

			d = dict(zip(keys, vals))
			for key, value in d.items():
				item[key] = value
			items.append(item)

		import pdb; pdb.set_trace()

		# return items

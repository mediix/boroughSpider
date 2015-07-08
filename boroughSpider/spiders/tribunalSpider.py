from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.item import DictItem, Field

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

		fields.update({'domain': Field()})
		fields.update({'borough': Field()})
		fields.update({'documents_url': Field()})
		return type(class_name, (DictItem,), {'fields':fields})

	def parse(self, response):
		links = response.xpath("//p[text()='View decision numbers: ']//@href").extract()

		# for link in links:
		url = '{0}{1}'.format(self.base_url, str(links[0]))
		yield FormRequest(url, method="GET", callback=self.parse_result)

	def parse_result(self, response):
		strat = (parse_html,)

		tab = extract(response.body, strategy=strat)
		table = list(prototypes.convert_table(tab.xpath("//table")))[0]

		table = { key.replace(' ', '_').lower(): value[0].encode('utf-8') for key, value in table.items() }

		TribunalItem = self.create_item_class('TribunalItem', table.keys())

		item = TribunalItem()

		for key, value in table.items():
			try:
				if value == '':
					item[key] = 'EMPTY'
				else:
					item[key] = parser.parse(str(value)).strftime("%Y-%m-%d")
			except:
				item[key] = value

		import pdb; pdb.set_trace()

		# return item


import MySQLdb
from scrapy.spider import Spider
from scrapy.shell import inspect_response
from scrapy.http import FormRequest
from scrapy.item import DictItem, Field
from scrapy.crawler import CrawlerProcess
from bs4 import BeautifulSoup

class tribunalSpider(Spider):
	name = 'tribSpider'
	pipeline = 'Tribunal'
	base = 'http://www.lease-advice.org/lvtdecisions/'
	start_urls = ['http://www.lease-advice.org/lvtdecisions/tables.asp?table=3']

	def create_item_class(self, class_name, field_list):
		fields = {}
		for field_name in field_list:
			fields[field_name] = Field()
		return type(class_name, (DictItem,), {'fields':fields})

	def parse(self, response):
		links = response.xpath("//p[text()='View decision numbers: ']//@href").extract()

		for link in links:
			url = '{0}{1}'.format(self.base, str(link))
			yield FormRequest(url, method="GET", callback=self.parse_result)

	def parse_result(self, response):
		# inspect_response(response, self)
		soup = BeautifulSoup(response.body, 'html.parser')
		table = soup.find('table', {'class': 'lvtFullTable'})
		keys = []
		for th in table.findAll('tr')[1]:
			if th.get_text(strip=True) in keys:
				keys.append(str(th.get_text(strip=True)) + '_')
			else:
				keys.append(str(th.get_text(strip=True)).replace(' ', '_'))

		tribunalItem = self.create_item_class('tribunalItem', keys)

		items = []

		for tr in table.findAll('tr'):
			vals = []
			for td in tr.findAll('td'):
				vals.append(td.get_text(strip=True).encode('ascii', 'ignore'))

			dic = dict(zip(keys, vals))
			try:
				dic.update({'Download': '{0}{1}'.format(self.base, tr.find('a').get('href').replace('../', ''))})
			except:
				pass
			item = tribunalItem()
			for key, value in dic.items():
				item[key] = value

			items.append(item)

		return items

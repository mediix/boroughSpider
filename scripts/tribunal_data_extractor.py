import MySQLdb, re
import urllib2
import requests
from bs4 import BeautifulSoup
from scrapy.item import DictItem, Field
from libextract import extract
from libextract.tabular import parse_html
# from lxml.html import open_in_browser

def download_pdf(url):
	'''
	link: pdf URL to be downloaded
	returns: writes file in the folder
	'''
	print url
	response = urllib2.urlopen(url)
	file_name = url.split('/')[-1]
	f = open(missing_files_storage + file_name, 'wb')
	f.write(response.read())
	f.close()
	print "Download and write data for %s Completed" % (url)

def write_items(link):
	'''
	link: html link to parse data from
	returns: writes data into the database
	'''
	ln = requests.get(link)
	ln.encoding = 'utf-8'
	soup = BeautifulSoup(ln.text)
	table = soup.find('table', {'class': 'lvtFullTable'})

	keys = []
	for th in table.findAll('th'):
		if th.get_text(strip=True) in keys:
			keys.append(str(th.get_text(strip=True)) + '_')
		else:
			keys.append(str(th.get_text(strip=True)).replace(' ', '_').replace('%', 'percentage'))

	items = []
	for tr in table.findAll('tr')[2:]: # skipping first two rows (table bar and header)
		vals = []
		for td in tr.findAll('td'):
			vals.append(td.get_text(strip=True).encode('ascii', 'ignore'))
		dic = dict(zip(keys, vals))
		try:
			dic.update({'Download': str(base_1 + tr.find('a').get('href').replace('../', ''))})
		except:
			dic.update({'Download': 'N/A'})
		items.append(dic)

	con = MySQLdb.connect(user='scraper',
		passwd='12345678',
		db='research_uk',
		host='granweb01',
		charset="utf8",
		use_unicode=True)
	cur = con.cursor()

	cur.execute("SHOW COLUMNS FROM leaseholds")
	cols = cur.fetchall()
	chk = lambda col: re.match(r'^\w+$', col)
	cols = [str(x[0]) for x in cols][1:]
	colnames = ','.join("`%s`" %col if not chk(col) else '%s' %col for col in cols)
	wildcards = ','.join(['%s'] * len(cols))

	"""
	Rows don't exist: 290, 1092, 1111, 1082, 1550, 2292, 1463
	"""

	for it in items:
		insert_sql = 'INSERT INTO leaseholds (%s) VALUES (%s)' % (colnames, wildcards)
		data = [it.get(col) for col in cols]
		try:
			cur.execute("SELECT id FROM addresses WHERE address = %s;", [it.get('Address')])
			addr_resp = cur.fetchone()
			if addr_resp is None:
				cur.execute("INSERT INTO addresses (address) VALUES (%s);", [it.get('Address')])
				con.commit()
				cur.execute("SELECT LAST_INSERT_ID();")
				addr_resp = cur.fetchone()
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])

		# print addr_resp
		data[0] = addr_resp[0]
		if int(data[1]) in missing_items:
			print 'insert: {0}\ndata: {1}'.format(insert_sql, tuple(data))
			cur.execute(insert_sql, tuple(data))
			con.commit()
			download_pdf(it.get('Download'))
		else:
			print "Not in missing_items"
	con.close()

if __name__ == "__main__":
	base = 'http://www.lease-advice.org/lvtdecisions/'
	base_1 = 'http://www.lease-advice.org/'
	files_storage = '/home/medi/UK_data/Medi/Tribunal/'
	missing_files_storage = '/home/medi/UK_data/Medi/missing_tribunal_items/'
	r = requests.get('http://www.lease-advice.org/lvtdecisions/tables.asp?table=3')
	strat = (parse_html,)
	tab = extract(r.content, strategy=strat)
	links = tab.xpath("//p[text()='View decision numbers: ']//@href")
	for link in links:
		# print base+link
		try:
			write_items(base + link)
		except:
			pass

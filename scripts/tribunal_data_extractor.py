import MySQLdb, re
import urllib2
import requests
from bs4 import BeautifulSoup
from scrapy.item import DictItem, Field
from libextract import extract
from libextract.tabular import parse_html

def write(link, cookie):
	'''
	link: html link to parse data from
	cookie: retrieved cookie
	returns: writes data into the database
	and downloads the target PDF
	'''
	ln = requests.get(link, cookies=cookie)

	soup = BeautifulSoup(ln.content, 'html.parser')

	table = soup.find('table', {'class': 'lvtFullTable'})

	keys = []
	for th in table.findAll('tr')[1]:
		if th.get_text(strip=True) in keys:
			keys.append(str(th.get_text(strip=True)) + '_')
		else:
			keys.append(str(th.get_text(strip=True)).replace(' ', '_').replace('%', 'percentage'))


	items = []
	for tr in table.findAll('tr'):
		vals = []
		for td in tr.findAll('td'):
			vals.append(td.get_text(strip=True).encode('ascii', 'ignore'))

		dic = dict(zip(keys, vals))
		try:
			dic.update({'Download': str(base_1 + tr.find('a').get('href').replace('../', ''))})
		except:
			dic.update({'Download': 'N/A'})
		items.append(dic)

	con = MySQLdb.connect(user='mehdi',
		passwd='pashmak.mN2',
		db='research_uk_public_data',
		host='granweb01',
		charset="utf8",
		use_unicode=True)
	cur = con.cursor()

	cur.execute("SHOW COLUMNS FROM leasehold")
	cols = cur.fetchall()

	chk = lambda col: re.match(r'^\w+$', col)
	cols = [str(x[0]) for x in cols][1:]
	colnames = ','.join("`%s`" %col if not chk(col) else '%s' %col for col in cols)
	wildcards = ','.join(['%s'] * len(cols))

	for it in items[2:]:
		data = tuple([it.get(col) for col in cols])
		# print data
		insert_sql = 'INSERT INTO leasehold (%s) VALUES (%s)' % (colnames, wildcards)
		cur.execute(insert_sql, data)
	con.commit()
	con.close()

	for it in items[2:]:
		url = it.get('Download')
		print url
		response = urllib2.urlopen(url)
		file_name = url.split('/')[-1]
		f = open(files_store + file_name, 'wb')
		f.write(response.read())
		f.close()
		f.close()
	print "Download and write data for %s Completed" % (base+str(link))

if __name__ == "__main__":
	base = 'http://www.lease-advice.org/lvtdecisions/'
	base_1 = 'http://www.lease-advice.org/'
	files_store = '/home/medi/UK_data/Medi/Tribunal/'

	r = requests.get('http://www.lease-advice.org/lvtdecisions/tables.asp?table=3')
	strat = (parse_html,)
	tab = extract(r.content, strategy=strat)
	links = tab.xpath("//p[text()='View decision numbers: ']//@href")

	for link in links:
		print base+link
		try:
			write(base+link, r.cookies.get_dict())
		except:
			pass

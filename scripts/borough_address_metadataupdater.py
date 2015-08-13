import MySQLdb

def fetch_addresses_for_stadiums(cur=None):
	""""""
	cur.execute("""SELECT a.id, ASTEXT(a.geom) AS geom
								 FROM addresses a
								 WHERE a.stadium IS NULL;""")

	addresses = cur.fetchall()

	return addresses

	# addresses = []

	# while True:
	# 	row = cur.fetchone()
	# 	if not row:
	# 		break;
	# 	addresses.append(row)


def update_address_for_stadium(address=None, cur=None):
	""""""
	cur.execute("""UPDATE addresses a
								 SET a.stadium = (SELECT s.id
								 									FROM stadiums s
								 									ORDER BY geo_distance(s.geom, GEOMFROMTEXT('{0}'))
								 									LIMIT 1)
								 WHERE a.id = {1};""".format(address[1], address[0]))

	print address[0]

if __name__ == '__main__':
	con = MySQLdb.connect(host='granweb01', port=3306, db='research_uk', user='scraper', passwd='12345678', charset="utf8", use_unicode=True)
	cur = con.cursor()

	addresses = fetch_addresses_for_stadiums(cur)

	# for address in addresses:
	# 	update_address_for_stadium(address, cur)

	# con.close()

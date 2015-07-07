import MySQLdb

def fetch_addresses_for_stadiums(con):
	cur = con.cursor()
	cur.execute('''
				SELECT a.id, ASTEXT(a.geom) AS geom
				FROM addresses a
				WHERE a.stadium IS NULL
				''')

	addresses = []

	while True:
		row = cur.fetchone()
		if not row:
			break;
		addresses.append(row)
	return addresses

def update_address_for_stadium(con, address):
	cur = con.cursor()
	cur.execute('''
				UPDATE addresses a
				SET a.stadium = (
						SELECT s.id
						FROM stadiums s
						ORDER BY geo_distance(s.geom, GEOMFROMTEXT('{0}'))
						LIMIT 1
					)
				WHERE a.id = {1};
				'''.format(address[1], address[0]))
	print address[0]


if __name__ == '__main__':
	con = MySQLdb.connect(host='granweb01', port=3306, db='research_uk', user='scraper', passwd='12345678', charset="utf8", use_unicode=True)
	addresses = fetch_addresses_for_stadiums(con)

	for address in addresses:
		update_address_for_stadium(con, address)

	con.close()
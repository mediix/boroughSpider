import MySQLdb
from geopy.geocoders import GoogleV3
from time import sleep

conn = MySQLdb.connect(user='scraper',
                      passwd='12345678',
                      db='research_uk',
                      host='granweb01',
                      charset="utf8",
                      use_unicode=True)
cursor = conn.cursor()
cursor.execute("""
              SELECT a.id, a.address
              FROM addresses a
              WHERE a.lat IS NULL OR a.lon IS NULL
              LIMIT 10;
              """)
addrs = cursor.fetchall()
addrs = { t[0]: t[1:] for t in addrs }
addrs = { key: (str(value[0]) if value else None) for key, value in addrs.items() }

g = GoogleV3('AIzaSyCQqiEtxqZVu7LOVdH5GhLqzn0dKexxxeg')
# address, (lat, lon) = g.geocode('15-16 Minories 62 Aldgate High Street London EC3 1AL', exactly_one=True)

def gpy(str):
  # sleep(0.35)
  addr, (lat, lon) = g.geocode(str, exactly_one=True)
  return [lat, lon]

addrsp = { key: gpy(value) for key, value in addrs.items() }

for key, value in addrsp.items():
  try:
    cursor.execute("""UPDATE addresses a
                  SET a.lat = %s, a.lon = %s
                  WHERE a.id = %s""",
                  (str(value[0]), str(value[1]), str(key)))
    conn.commit()
  except MySQLdb.Error, e:
    print "ERROR %d: %s" % (e.args[0], e.args[1])
    conn.rollback()
import MySQLdb, sys
from geopy.geocoders import GoogleV3
from time import sleep

conn = MySQLdb.connect(user='scraper',
                      passwd='12345678',
                      db='research_uk',
                      host='granweb01',
                      charset="utf8",
                      use_unicode=True)
cursor = conn.cursor()
cursor.execute("""SELECT a.id, a.address
                  FROM addresses a
                  WHERE a.is_geocoded = 0
                    AND (a.lat IS NULL OR a.lon IS NULL)
                  LIMIT 2500;""")
addrs = cursor.fetchall()
addrs = { t[0]: t[1:] for t in addrs }
addrs = { key: (str(value[0]) if value else None) for key, value in addrs.items() }

'''
Scraper's google account:
  scraper.gvhomes@gmail.com
  pass: scraper12345678
'''
g = GoogleV3('AIzaSyCaENUu85uuSC6h8-1DhJ5H29R0O0WrFqA')
# address, (lat, lon) = g.geocode('15-16 Minories 62 Aldgate High Street London EC3 1AL', exactly_one=True)

for key, value in addrs.items():
  try:
    response = g.geocode(value, exactly_one=True)
    if response is not None:
      addr, (lat, lon) = response
      cursor.execute("""UPDATE addresses a
                  SET a.lat = %s, a.lon = %s, a.address_adjusted = %s
                  WHERE a.id = %s""",
                  [lat, lon, addr, key])
      conn.commit()
      sys.stdout.write('.')
      sys.stdout.flush()
  except MySQLdb.Error, e:
    print "ERROR %d: %s" % (e.args[0], e.args[1])
    conn.rollback()
  except:
    print 'exception: ', sys.exc_info()[0]
  finally:
    cursor.execute("""UPDATE addresses a
                    SET a.is_geocoded = 1
                    WHERE a.id = %s""", [key])
    conn.commit()

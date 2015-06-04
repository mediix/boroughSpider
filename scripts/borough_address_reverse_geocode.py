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
                LIMIT 2500;
                """)
addrs = cursor.fetchall()
addrs = { t[0]: t[1:] for t in addrs }
addrs = { key: (str(value[0]) if value else None) for key, value in addrs.items() }

#g = GoogleV3('AIzaSyCQqiEtxqZVu7LOVdH5GhLqzn0dKexxxeg')
g = GoogleV3('AIzaSyBjF-Ks8Q2VoxczLdtQLEOlu2wL426yjgk')
# address, (lat, lon) = g.geocode('15-16 Minories 62 Aldgate High Street London EC3 1AL', exactly_one=True)

for key, value in addrs.items():
  try:
    response = g.geocode(value, exactly_one=True)
    if not response is None:
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

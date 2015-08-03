import MySQLdb, sys
from geopy.geocoders import GoogleV3
from time import sleep

def address_geocoder():
  '''
  database: target database
  table: target table
  returns: updated lat, lon columns within the database

  #GOOGLE's Account for scraper:
    Scraper's google account:
    scraper.gvhomes@gmail.com
    pass: scraper12345678
  '''
  conn = MySQLdb.connect(user='scraper',
                        passwd='12345678',
                        db='research_uk',
                        host='granweb01',
                        charset="utf8",
                        use_unicode=True)
  cursor = conn.cursor()
  cursor.execute("""SELECT id, address
                    FROM addresses
                    WHERE is_geocoded = 0
                      AND (lat IS NULL OR lon IS NULL)
                    LIMIT 2500;""")
  addrs = cursor.fetchall()
  addrs = { t[0]: t[1:] for t in addrs }
  addrs = { key: (value[0].encode('utf-8') if value else None) for key, value in addrs.items() }

  g = GoogleV3('AIzaSyCaENUu85uuSC6h8-1DhJ5H29R0O0WrFqA')

  for key, value in addrs.items():
    try:
      response = g.geocode(value, exactly_one=True)
      if response is not None:
        addr, (lat, lon) = response
        print "Address: {0}, Latitude: {1}, Longitutde: {2}".format(addr, lat, lon)
        cursor.execute("""UPDATE addresses
                          SET lat = %s, lon = %s, address_adjusted = %s
                          WHERE id = %s""",
                          [lat, lon, addr, key])
        conn.commit()
        sys.stdout.write('.')
        sys.stdout.flush()
    except (MySQLdb.Error, MySQLdb.OperationalError) as e:
      print "ERROR %d: %s" % (e.args[0], e.args[1])
      conn.rollback()
    except:
      print 'exception: ', sys.exc_info()[0]
    finally:
      cursor.execute("""UPDATE addresses
                        SET is_geocoded = 1
                        WHERE id = %s;""", [key])
      conn.commit()
      conn.close()

if __name__ == '__main__':
  address_geocoder()

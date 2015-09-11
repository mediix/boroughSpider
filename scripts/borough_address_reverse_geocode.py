import MySQLdb, sys
from geopy.geocoders import GoogleV3
from time import sleep

API_keys = ['AIzaSyDNQZxPSQ_gfpQE9sVGmIwKOnbRjcEilLA',
            'AIzaSyAoSpAegjLFNPipFLHD8MtmpH54YomhReg',
            'AIzaSyDVFoT_bWDgBArqiYGsf33GYfBfxZn4qbM',
            'AIzaSyDVq6OFHPVoySHcVTKcFO6t_7MVcwlsX6k',
            'AIzaSyDOpsJAlBbyhY2sZBPXom8HuHmDsegfIwU',
            'AIzaSyBwYIzekGVDThlIQg4amlqgDp-YI_NFjb0',
            'AIzaSyCJs1jceSdVwvVU84JxR785bxJhEnuLONU',
            'AIzaSyD2YF6Fq5_VFZWaahsAFU_eBfe7DTFVsZU',
            'AIzaSyCWHVugW0e6jYgIBAAbRtHIb6sEgz2WDFk',
            'AIzaSyCaRBOchmd_Ge726PnAot9NwlO_u6kmnDA']

def address_geocoder(API_key=None):
  """
  database: target database
  table: target table
  returns: updated lat, lon columns within the database

  Google account for scraper:
    user: scraper.gvhomes@gmail.com
    pass: scraper12345678
  """
  con = MySQLdb.connect(user='scraper',
                        passwd='12345678',
                        db='research_uk',
                        host='granweb01',
                        charset='utf8',
                        use_unicode=True)
  cur = con.cursor()
  cur.execute("""SELECT id, address
                 FROM addresses
                 WHERE is_geocoded = 0
                    AND (lat IS NULL OR lon IS NULL)
                 LIMIT 2500;""")
  addrs = cur.fetchall()
  addrs = { t[0]: t[1:] for t in addrs }
  addrs = { key: (value[0].encode('utf-8') if value else None) for key, value in addrs.items() }

  geo = GoogleV3(API_key)

  for key, value in addrs.items():
    sleep(2)
    try:
      response = geo.geocode(value, exactly_one=True)
      if response is not None:
        addr, (lat, lon) = response
        # print "Address: {0}, Latitude: {1}, Longitutde: {2}".format(addr, lat, lon)
        cur.execute("""UPDATE addresses
                          SET lat = %s, lon = %s, address_adjusted = %s, is_geocoded = 1
                          WHERE id = %s""",
                          [lat, lon, addr, key])
        con.commit()
        sys.stdout.write('.')
        sys.stdout.flush()
    except (MySQLdb.Error, MySQLdb.OperationalError) as err:
      print "MySQLdb Error %d: %s" % (err.args[0], err.args[1])
      con.rollback()
    except Exception as err:
      print "General Error: ", err


if __name__ == '__main__':
  for key in API_keys[::-1]:
    try:
      address_geocoder(key)
    except Exception:
      break
    finally:
      con.close()

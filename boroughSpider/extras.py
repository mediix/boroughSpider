import MySQLdb

default = ''

db_Error = MySQLdb.Error
# con = MySQLdb.connect(user='scraper',
#                  passwd='12345678',
#                  db='research_uk',
#                  host='granweb01',
#                  charset='utf8',
#                  use_unicode=True)

# con = MySQLdb.connect(user='mehdi',
#                   passwd='pashmak.mN2',
#                   db='research_uk_public_data',
#                   host='granweb01',
#                   charset='utf8',
#                   use_unicode=True)

con = MySQLdb.connect(user='root',
                  passwd='pashmak.',
                  db='research_uk',
                  host='rappi.local',
                  charset='utf8',
                  use_unicode=True)
cur = con.cursor()

def check_address(address):
  """"""
  sql = """SELECT id FROM addresses WHERE address = "%s";""" % (address,)
  try:
    cur.execute(sql)
    db_response = cur.fetchone()
    if db_response is None:
      cur.execute("INSERT INTO addresses (address) VALUES (%s);", [address])
      con.commit()
      cur.execute("SELECT LAST_INSERT_ID();")
      db_response = cur.fetchone()
    _id = db_response[0]
  except db_Error as err:
    print "Error: %d: %s" % (err.args[0], err.args[1])

  return _id

# def store_keys(item, name):
#     """"""
#     file_name = file_path.replace('NAME', name)
#     item_keys = item

#     if not os.path.isfile(file_name):
#         with open(file_name, 'w') as f:
#             json.dump(item_keys, f)
#     try:
#         with open(file_name, 'r') as f:
#             stored_keys = json.load(f)
#     except Exception as err:
#         print "Error opening file from store_kyes: ", err
#         pass
#     if stored_keys:
#         diff = list(set(item_keys) - set(stored_keys))
#         if diff:
#             print "\n!!!DIFF IS: \n", diff
#             stored_keys = stored_keys + diff
#             with open(file_name, 'w') as f:
#                 json.dump(stored_keys, f)
#     else:
#         stored_keys = item_keys
#         with open(file_name, 'w') as f:
#             json.dump(stored_keys, f)



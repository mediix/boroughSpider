# fetch data from packages
import MySQLdb
import urllib2, json
from pandas.io import sql
import pandas as pd
from pandas import DataFrame
from dateutil import parser
# from pandas.io.json import json_normalize

dbtypes = {
    'mysql' : {'DATE':'DATE', 'DATETIME':'DATETIME', 'INT':'BIGINT', 'FLOAT':'FLOAT', 'VARCHAR':'VARCHAR'}
    }

def write_dict(dict, con=None, flavor='msyql', if_exists='replace'):
  '''
  dict: dictionary formatted data to be stored
  con: dbms connection
  if_exists:
    'fail'    - create table will be attempted and fail
    'replace' - if table with 'name' exists, deleted
    'append'  - assume table with correct schema exists and add data
  returns: writes a dict into MySQL database
  '''
  pass

def get_schema():
  '''
  returns: relational schema  of dict
    with respect to the values associated to
    the dictionary.
  '''
  pass

def stringify(input):
  '''
  input: JSON object
  returns: UTF-8 encoded json
  '''
  if isinstance(input, dict):
      return {byteify(key): byteify(value) for key, value in input.iteritems()}
  elif isinstance(input, list):
      return [byteify(element) for element in input]
  elif isinstance(input, unicode):
      return input.encode('utf-8')
  else:
        return input

def check(val):
  '''
  val: 'list', 'dict', 'bool', 'int', 'None'
  returns: dictionary
  '''
  # print val
  date_parse = lambda x: parser.parse(str(x)).strftime("%Y-%m-%d")
  for k, v in val.items():
    if isinstance(v, dict):
      # print 'val is dictionary', type(v)
      val[k] = None
      return check(v)
    elif isinstance(v, list):
      # print 'val is list', type(v)
      return val[k] = ','.join(i for i in v)
    elif isinstance(v, bool):
      print 'val is boolean', type(v)
    elif isinstance(v, int):
      print 'val is integer', type(v)
    elif v is None:
      print 'val is NoneType', type(v)
    else:
      print v.decode('utf-8', 'ignore')

if __name__ == '__main__':
  con = MySQLdb.connect(user='scraper',
  		passwd='12345678',
  		db='research_uk_public_data',
  		host='granweb01',
  		charset="utf8",
  		use_unicode=True)

  dataset_names='http://data.london.gov.uk/api/3/action/package_list'
  dataset='http://data.london.gov.uk/api/3/action/package_show?id='

  df = list(pd.read_json(dataset_names)['result'])
  dsets = [d.encode('utf-8') for d in df]

  cur = con.cursor()
  for dset in dsets[:1]:
    dset = dict(pd.read_json(dataset + dset)['result'])
    dset = stringify(dset)
    # cur.execute("""INSERT INTO packages (title)
                # VALUES (%s)""", [dset['title'].encode('utf-8')])
    # con.commit()

    data = {}
    if dset['resources']:
    	data.update({'resources': dset['resources'][0]})
    if dset['organization']:
    	data.update({'organization': dset['organization']})
    if dset['tags']:
      data.update({'tags': dset['tags'][0]})
    if dset['groups']:
      data.update({'groups': dset['groups'][0]})

    # for k, v in data.items():
      # check(v)

    # data = { k.encode('utf-8'): check(v) else None for k, v in data.items() }

    ddf = DataFrame.from_dict(data, columns=data.keys())


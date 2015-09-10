import MySQLdb, re
import urllib2
from urllib import urlencode
import requests
from bs4 import BeautifulSoup

con = MySQLdb.connect(user='mehdi',
                   passwd='pashmak.mN2',
                   db='research_uk_public_data',
                   host='granweb01',
                   charset='utf8',
                   use_unicode=True)
cur = con.cursor()

files_storage = '/home/medi/UK_data/Medi/Application_Forms/Hammersmith&Fulham/'
base_url = 'http://public-access.lbhf.gov.uk'

def doc_extract(url=None, name=None):
  """"""
  # print "URL: ", url
  with requests.Session() as s:
    r = s.get(url)
    c = s.cookies.get_dict()
  r.encoding = 'utf-8'
  soup = BeautifulSoup(r.text)

  """
  Table tags for depending on boroughs Documents tab.
  """
  # table = soup.find('table', {'id': 'casefiledocs'})  # kensigton
  table = soup.find('table', {'id': 'Documents'})       # City of Westminster/Hammersmith

  ###
  vals = []
  rows = table.find_all('tr')
  for row in rows:
    for td in row.find_all('td'):
      if td.text == 'Application Form':
        vals.append(base_url+row.find('a').get('href'))

  fk = lambda x: x.replace('/', '_')
  if len(vals) > 1:
    for idx, it in enumerate(vals):
      try:
        response = requests.get(it, cookies=c)
        f = open(files_storage + fk(name) + '_' + str(idx+1) + '.pdf', 'wb')
        f.write(response.content)
      except Exception as err:
        raise IOError('Error Writing into File', err)
      else:
        print "%s Download Completed. :)" % (fk(name)+'_'+str(idx+1)+'.pdf')
        f.close()
  elif len(vals) == 1:
    try:
      response = requests.get(vals[0], cookies=c)
      f = open(files_storage + fk(name) + '_application_form' + '.pdf', 'wb')
      f.write(response.content)
    except Exception as err:
      raise IOError('Error Writing to File', err)
    else:
      print "%s Download Completed. :)" % (fk(name)+'_application_form'+'.pdf')
      f.close()
  else:
    raise ValueError('No Document Link to Download!')

#----------------------------------------------------------------------------------
if __name__ == '__main__':
  #
  cur.execute("""SELECT case_reference_borough, documents_url
                 FROM boroughs
                 WHERE borough = 'Hammersmith & Fulham'
                      AND has_application = 0
                      AND documents_url IS NOT NULL
                      AND case_reference_borough IS NOT NULL;""")
  result = cur.fetchall()
  result = { t[0]: t[1:] for t in result }
  result = { k.encode('utf-8'): None if not v[0] else v[0].encode('utf-8') for k, v in result.items() }

  intify = lambda x: 1 if len(x) > 0 else 0
  for key, value in result.items():
    try:
      doc_extract(value, key)
    except Exception as err:
      print "Unable to download the documents."
      continue
    else:
      cur.execute("""UPDATE boroughs
                     SET has_application = %s
                     WHERE case_reference_borough = %s;""", [intify(value), key.encode('utf-8')])
      con.commit()


""" Hammersmith and Fulham
vals = []
  rows = table.find_all('tr')
  for row in rows:
    for td in row.find_all('td'):
      if td.text == 'Application Form':
        vals.append(base_url+row.find('a').get('href'))
"""

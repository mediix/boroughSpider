import MySQLdb, re
import urllib2
from urllib import urlencode
import requests
from bs4 import BeautifulSoup

con = MySQLdb.connect(user='mehdi', passwd='pashmak.mN2', db='research_uk', host='granweb01', charset="utf8", use_unicode=True)

files_storage = '/home/medi/UK_data/Medi/Application_documents_1/'
base_url = 'http://public-access.lbhf.gov.uk/'

def doc_extract(url=None):
  """"""
  # print "URL: ", url
  with requests.Session() as s:
    r = s.get(url)
    c = s.cookies.get_dict()
  # ln = requests.get(url)
  r.encoding = 'utf-8'
  soup = BeautifulSoup(r.text)
  # table = soup.find('table', {'id': 'casefiledocs'})  # kensigton
  table = soup.find('table', {'id': 'Documents'})       # City of Westminster
  #
  keys = []
  for th in table.findAll('th'):
    keys.append(str(th.get_text(strip=True)).lower().replace(' ', '_'))

  fk = lambda x: x.replace('/', '_')
  # data = []
  for tr in table.findAll('tr')[1:]: # skip the table header
    vals = []
    for td in tr.findAll('td'):
      if td.find('a'):
        vals.append(base_url + td.find('a').get('href'))
      else:
        vals.append(td.get_text(strip=True))
    #
    data = dict(zip(keys, vals))

    if data.has_key('document_type'):
      if data.get('document_type') == 'Application Form':
        # for idx, it in enumerate(item):
        #   try:
        #     name = files_storage + fk(key) + file_name + '_' + str(idx+1) + ext
        #     # url = it
        #     # url_parts = url.split('?')
        #     # url_parts[1] = urlencode({'test':url_parts[1]})
        #     # url = '{0}?{1}'.format(url_parts[0], url_parts[1])
        #     # print it
        #     response = requests.get(it, cookies=c)
        #     f = open(name, 'wb')
        #     f.write(response.content)
        #   except Exception, e:
        #     print "ERROR FROM download_resource: elif: ", e
        #   else:
        #     print "%s Download Completed" % (fk(key)+file_name+'_'+str(idx+1)+ext)
        #     f.close()
        try:
          response = requests.get(data.get('view'), cookies=c)
          f = open(files_storage + fk(key) + '_application_form' + '.pdf', 'wb')
          f.write(response.content)
        except Exception as e:
          print "ERROR FROM download_source: else", e
        else:
          print "%s Download Completed" % (fk(key)+'_application_form'+'.pdf')
          f.close()



# def download_resource(data=None, cookie=None):
#   """"""
#   key = data[0]
#   item = data[1]
#   file_name = '_application_form'
#   ext = '.pdf'
#   fk = lambda x: x.replace('/', '_')

#   if len(item) == 0:
#     print "NO file to download"
#   elif len(item) > 1:
#     for idx, it in enumerate(item):
#       try:
#         name = files_storage + fk(key) + file_name + '_' + str(idx+1) + ext
#         # url = it
#         # url_parts = url.split('?')
#         # url_parts[1] = urlencode({'test':url_parts[1]})
#         # url = '{0}?{1}'.format(url_parts[0], url_parts[1])
#         # print it
#         response = requests.get(url, cookie)
#         f = open(name, 'wb')
#         f.write(response.content)
#       except Exception, e:
#         print "ERROR FROM download_resource: elif: ", e
#       else:
#         print "%s Download Completed" % (fk(key)+file_name+'_'+str(idx+1)+ext)
#         f.close()
#   else:
#     try:
#       print item
#       response = requests.get(item[0], cookie)
#       f = open(files_storage + fk(key) + file_name + ext, 'wb')
#       f.write(response.content)
#     except Exception as e:
#       print "ERROR FROM download_source: else", e
#     else:
#       print "%s Download Completed" % (fk(key)+file_name+ext)
#       f.close()

#----------------------------------------------------------------------------------
if __name__ == '__main__':
  ###
  def select(data=None):
    """"""
    vals = []
    for key, value in data.items():
      for elem in value:
        for k, v in elem.items():
          if v == 'Application Form':
            vals.append(elem.get('view'))

    return (key, vals)

  cur = con.cursor()
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
    # res = {}
    try:
      doc_extract(value)
    except Exception as err:
      print "ERROR FROM => main", err
    else:
      cur.execute("""UPDATE boroughs
                     SET has_application = %s
                     WHERE case_reference_borough = %s;""", [intify(value), key.encode('utf-8')])
      con.commit()

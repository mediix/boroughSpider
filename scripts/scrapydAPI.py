from requests import post
from dateutil import parser
from datetime import date, datetime, timedelta
from scrapyd_api import ScrapydAPI

def create_dates(start, end, delta):
  curr = start
  while curr < end:
    yield curr
    curr += delta

months = []
for result in create_dates(date(2013, 8, 1), date(date.today().year, (date.today().month+1)%12, 1), timedelta(days = 31)):
  months.append(result.strftime('%b %y'))

boroughs = {'CityOfLondon': {'domain':'http://www.cityoflondon.gov.uk',
                             'start_urls':'http://www.planning2.cityoflondon.gov.uk/online-applications/search.do?action=monthlyList',
                             'borough':'CityOfLondon',
                             'base_url':'http://www.planning2.cityoflondon.gov.uk'},
            'Lambeth': {'domain':'http://planning.lambeth.gov.uk',
                        'start_urls':'http://planning.lambeth.gov.uk/online-applications/search.do?action=monthlyList',
                        'borough':'Lambeth',
                        'base_url':'http://planning.lambeth.gov.uk'}}

#---------------------------------------------------------------------------------
p = 'boroughSpider'
scrapyd = ScrapydAPI('http://localhost:6800')

# [scrapyd.delete_version(p, ver) for ver in scrapyd.list_versions(p)]

# for elem in scrapyd.list_jobs(p)['running']:
#   print elem
#   scrapyd.cancel(p, elem.get('id'))

spiders = [spider.encode('utf-8') for spider in scrapyd.list_spiders(p)]

for month in months[::-1]:
  for sp in spiders:
    if sp == 'lambSpider':
      data = {'project':p, 'spider':sp, 'month':month}
      post('http://localhost:6800/schedule.json', data=data)

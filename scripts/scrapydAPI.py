from requests import post
# from dateutil import parser
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

#---------------------------------------------------------------------------------
p = 'boroughSpider'
scrapyd = ScrapydAPI('http://localhost:6800')
#scrapyd = ScrapydAPI('http://rappi.local:6800')

# [scrapyd.delete_version(p, ver) for ver in scrapyd.list_versions(p)]

# for elem in scrapyd.list_jobs(p)['running'] or scrapyd.list_jobs(p)['pending']:
#   print elem
#   scrapyd.cancel(p, elem.get('id'))

spiders = [spider.encode('utf-8') for spider in scrapyd.list_spiders(p)]

for sp in spiders:
  # if sp in ['kensSpider', 'camdSpider', 'hackSpider', 'isliSpider', 'towerSpider', 'wandSpider']:
  if sp == 'kensSpider':
    data = {'project':p, 'spider':sp}
    post('http://localhost:6800/schedule.json', data=data)


# for month in months[::-1]:
#   for sp in spiders:
#     if sp in ['hammSpider', 'londSpider', 'westSpider', 'soutSpider', 'lambSpider']:
#     # if sp == 'londSpider':
#       data = {'project':p, 'spider':sp, 'month':month}
#       post('http://localhost:6800/schedule.json', data=data)


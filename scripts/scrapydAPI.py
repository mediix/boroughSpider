import os
# from subprocess import Popen, PIPE
from datetime import date, datetime, timedelta
from scrapyd_api import ScrapydAPI

p = 'boroughSpider'
scrapyd = ScrapydAPI('http://localhost:6800')

def create_dates(start, end, delta):
  curr = start
  while curr < end:
    yield curr
    curr += delta

if __name__ == '__main__':
  months = []
  for result in create_dates(date(2013, 1, 1), date.today(), timedelta(days = 31)):
    months.append(result.strftime('%b %y'))

  ## Delete Version
  # [scrapyd.delete_version(p, ver) for ver in scrapyd.list_versions(p)]

  ## Spiders list
  spiders = [spider.encode('utf-8') for spider in scrapyd.list_spiders(p)]

  for month in months[:2]:
    for spider in spiders:
      if spider == 'londSpider':
        scrapyd.schedule(p, spider)



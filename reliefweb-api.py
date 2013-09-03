import requests
import datetime
import orm

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
"""

def yeartotimestamp(year):
    d = datetime.datetime(year=year, month=1, day=1)
    return int(d.strftime('%s'))

def getcountrylist():
    for value in orm.session.query(orm.Value).filter(orm.Value.indID == "m49-name").all():
        yield value.region

dsID="reliefweb-api"

dataset = {'dsID': dsID,
           'last_updated': orm.now()
           'last_scraped': orm.now()
           'name': "ReliefWeb API"}

orm.DataSet(**dataset).save()

baseurl = i


"http://api.rwlabs.org/v0/report/list?limit=1&query[value]={country}&query[fields][0]=country&filter[field]=date.created&filter[value][from]={from}000&filter[value][to]={to}000"


query = {"filter": 
            {"operator": "AND",
             "conditions":
                 [
                     {'field': 'date.created',
                      'value': {'from': FROM, 'to': TO}
                     },
                     {'field': 'country',
                      'value': COUNTRY
                     },
                     {'field': 'title',
                      'value': TITLE}
                 ]

iparams = {"country": "SDN",
          "from": 0,
          "to": 1}

countries = list(getcountrylist())

for 
for year in range(1990,2014):
    params['from'] = yeartotimestamp(year)
    params['to'] = yeartotimestamp(year+1)-1
    url = baseurl.format(**params)
    print year, requests.get(url).json()['data']['total']

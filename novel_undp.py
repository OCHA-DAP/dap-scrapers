import re
import datetime
import requests
from orm import session, Value, DataSet, Indicator
import orm
"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

dsID = "data.undp.org"

dataset = {"dsID": dsID,
           "last_updated": None,  # TODO max(pubdate)
           "last_scraped": orm.now(), 
           "name": "UNDP Open Data"}
DataSet(**dataset).save()

data_url = "http://data.undp.org/resource/{}.json"

lookup = [{'soc':'ku9i-8fxp', 
           'fieldname':'gender_inequality_index_value_2013',
           'indID':'HDR:68606',
           'unit': 'index'}]

#lookup = {
#          "ku9i-8fxp": "HDR:68606",  # GII: Gender Inequality Index, value
          #"u2dx-####": "PSE110",  # GNI per capita in PPP terms (constant 2005 international $)
          #"bkr7-####": "PVE010",  # Public expenditure on education (% of GDP) (%)
          #"m67k-####": "PVE110",  # Mean years of schooling (of adults)|years
          #"jbhn-####": "PVE120",  # Combined gross enrolment in education (both sexes)
          #"ehe9-####": "PSE160",  # MPI: Population living below $1.25 PPP per day (%)
          #"a4ay-####": "PVH120",  # Under-five mortality
          #"qnam-####": "PVE030",  # Expected Year of Schooling (of children)
          #"4gkx-####": "PVH180",  # Maternal mortality ratio
          #"x22y-####": "PVE040",  # Adult literacy rate, both sexes (% aged 15 and above)

         # "---------": "------",  # Impact of natural disasters: number of deaths
         # "XXXXXXXXX": "PSE220",  # Human Development Index rank (in all of them)
         # "=========": "PVX070",  # Impact of natural disasters: population affected
 #        }

def get_period(s):
    p = re.findall('(?:19|20)\d\d', s)
    if len(p) == 0:
        return '2014'
    assert len(p) == 1
    return p[0]

class SocrataData(object):
    ROWS_URL = "https://data.undp.org/api/views/{}/rows.json?accessType=DOWNLOAD"
    pass
    def __init__(self, id):
        self.id = id
        self.url = self.ROWS_URL.format(id)
        self.jdata = requests.get(self.url).json()

    @property
    def columns(self):
        # name, fieldName
        return self.jdata['meta']['view']['columns']

    def name_for_fieldname(self, fieldname):
        try:
            name, = [x['name'] for x in self.columns if x['fieldName'] == fieldname]
        except:
            print x
            raise

    @property
    def column_fieldnames(self):
        return [x['fieldName'] for x in self.columns]
 
    @property
    def rows(self):
        return self.jdata['data']

    @property
    def combined(self):
        return [dict(zip(self.column_fieldnames, row)) for row in self.rows]

    def extract(self, fieldname):
        return [{'region': x['country'], 'value': x[fieldname]} for x in self.combined]

    def export(self, meta):
        ind = {'indID': meta['indID'],
               'name': self.name_for_fieldname(meta['fieldname']), 
               'units': meta['unit']}
        Indicator(**ind).save()


        for item in self.extract(meta['fieldname']):
            value = {'dsID': dsID,
                     'region': item['region'],
                     'period': meta.get('period') or get_period(meta['fieldname']),
                     'value': item['value'],
                     'indID': meta['indID'],
                     'source': self.url,
                     'is_number': meta.get('is_number') or True}
            if item['region']:
                Value(**value).save()
        
soc = SocrataData('ku9i-8fxp')
# print soc.extract('gender_inequality_index_value_2013')
soc.export(lookup[0])


"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

#	yield {"dsID": dsID,
#	       "region": country['country'],
#	       "period": int(key[1:]),
#	       "value": float(country[key]),
#	       "indID": lookup[socrata_id],
#	       "source": data_url.format(socrata_id),
#	       "is_number": True}

#def parse_rank(socrata_id, countries):
#    for country in countries:
#        if 'hdi_rank' in country:
#	    yield {"dsID": dsID,
#		   "region": country['country'],
#		   "period": 2012,  # TODO
#		   "value": int(country['hdi_rank']),
#		   "indID": "PSE220",
#		   "source": data_url.format(socrata_id),
#		   "is_number": True}
		   
#DataSet(**dataset).save()
#for socrata_code in lookup:
#    ind = get_metadata(socrata_code)
#    Indicator(**ind).save()
#    for value in get_numbers(socrata_code):
#        Value(**value).save()
#
#print "rank"
#ind = {"indID": "PSE220",
#       "name": "HDI Rank",
#       "units": "rank"}
#Indicator(**ind).save()
#for rank in get_rank("u2dx-y6wx"):
#    Value(**rank).save()



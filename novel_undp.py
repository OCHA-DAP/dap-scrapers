import re
import datetime
import requests
from orm import session, Value, DataSet, Indicator
import orm
import logging

logging.basicConfig()

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
           'unit': 'Index'},

          {'soc':'myer-egms',
           'fieldname':'_2013_gross_national_income_gni_per_capita_2011_ppp',
           'indID':'chd.eco.135',
           'unit': '2011 PPP $',
           'period': '2013'},

          {'soc':'xn26-t7qa',
           'fieldname':'expenditure_on_education_of_gdp_2005_2012',
           'indID':'PVE010',
           'unit': 'Percentage',
           'period': '2005-2012'},

          {'soc':'5tuc-d2a9',
           'fieldname':'mean_years_of_schooling',
           'indID':'PVE110',
           'unit': 'Years'},

          {'soc':'5tuc-d2a9',
           'fieldname':'population_living_below_1_25_a_day',
           'indID':'PSE160',
           'unit': 'Percentage'},

          {'soc':'5tuc-d2a9',
           'fieldname':'expected_years_of_schooling',
           'indID':'PVE030',
           'unit': 'Years'},
          
          {'soc':'5tuc-d2a9',
           'fieldname':'maternal_mortality_ratio_deaths_per_100_000_live_births',
           'indID':'PVH180',
           'unit':'Deaths per 100,000 live births'},

          {'soc':'5tuc-d2a9',
           'fieldname':'adult_literacy_rate_ages_15_and_older',
           'indID':'PVE040',
           'unit':'Percentage'},

          {'soc':'5tuc-d2a9',
           'fieldname':'impact_of_natural_disasters_number_of_deaths_per_year_per_million_people',
           'indID':'not_known',
           'unit':'People affected per 1,000,000 people per year'},

          {'soc':'sf29-qtcx',
           'fieldname':'effects_of_environmental_threats_impact_of_natural_disasters_population_affected_per_year_per_million_people_2005_2012',
           'indID':'PVX070',
           'unit':'People affected per 1,000,000 people per year',
           'period':'2005-2012'},

          {'soc':'myer-egms',
           'fieldname':'hdi_rank',
           'indID':'PSE220',
           'unit':'Index'},

          {'soc':'5tuc-d2a9',
           'fieldname':'under_five_mortality_rate',
           'indID': 'PVH120',
           'unit': 'Deaths per 1,000 live births'},

]

#lookup = {
#          OK "ku9i-8fxp": "HDR:68606",  # GII: Gender Inequality Index, value
          #OK "u2dx-####": "PSE110",  # GNI per capita in PPP terms (constant 2005 international $)
          #OK"bkr7-####": "PVE010",  # Public expenditure on education (% of GDP) (%)
          #OK"m67k-####": "PVE110",  # Mean years of schooling (of adults)|years
   #TODO       #"jbhn-####": "PVE120",  # Combined gross enrolment in education (both sexes)
          #OK"ehe9-####": "PSE160",  # MPI: Population living below $1.25 PPP per day (%)
          #???"a4ay-####": "PVH120",  # Under-five mortality
          #OK"qnam-####": "PVE030",  # Expected Year of Schooling (of children)
          #OK"4gkx-####": "PVH180",  # Maternal mortality ratio
          #OK"x22y-####": "PVE040",  # Adult literacy rate, both sexes (% aged 15 and above)
          

         # "---------": "------",  # Impact of natural disasters: number of deaths
         # "XXXXXXXXX": "PSE220",  # Human Development Index rank (in all of them)
         # "=========": "PVX070",  # Impact of natural disasters: population affected
 #        }

def get_period(s):
    p = re.findall('(?:19|20)\d\d', s)
    if len(p) == 0:
        return '2014'
    assert len(p) == 1, "Many periods! {!r}".format(p)
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
        except Exception:
            print [x['fieldName'] for x in self.columns]
            print "Couldn't find {}".format(fieldname)
            raise
        return name

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
        try:
            return [{'region': x['country'], 'value': x[fieldname]} for x in self.combined]
        except Exception:
            return [{'region': x['countries'], 'value': x[fieldname]} for x in self.combined]

    def export(self, meta):
        ind = {'indID': meta['indID'],
               'name': self.name_for_fieldname(meta['fieldname']), 
               'units': meta['unit']}
        Indicator(**ind).save()

        for item in self.extract(meta['fieldname']):
            if not item.get('region'):
                logging.warn("No region in {}".format(meta))
                continue
            value = {'dsID': dsID,
                     'region': item['region'],
                     'period': meta.get('period') or get_period(meta['fieldname']),
                     'value': item['value'],
                     'indID': meta['indID'],
                     'source': self.url,
                     'is_number': meta.get('is_number') or True}
            if value['region'] and value['value']:
                print value
                Value(**value).save()
        
for item in lookup:
    soc_data = SocrataData(item['soc'])
# print soc_data.extract('gender_inequality_index_value_2013')
    soc_data.export(item)


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



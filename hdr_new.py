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

metadata_url = "https://data.undp.org/api/views/{}/rows.json?accessType=DOWNLOAD"
data_url = "http://data.undp.org/resource/{}.json"
lookup = {"u2dx-y6wx": "PSE110",  # GNI per capita in PPP terms (constant 2005 international $)
          "bkr7-unqh": "PVE010",  # Public expenditure on education (% of GDP) (%)
          "m67k-vi5c": "PVE110",  # Mean years of schooling (of adults)|years
          "jbhn-xkjv": "PVE120",  # Combined gross enrolment in education (both sexes)
          "ehe9-pgud": "PSE160",  # MPI: Population living below $1.25 PPP per day (%)
          "a4ay-qce2": "PVH120",  # Under-five mortality
          "bh77-rzbn": "HDR:68606",  # GII: Gender Inequality Index, value
          "qnam-f624": "PVE030",  # Expected Year of Schooling (of children)
          "4gkx-mq89": "PVH180",  # Maternal mortality ratio
         # "---------": "------",  # Impact of natural disasters: number of deaths
         # "XXXXXXXXX": "PSE220",  # Human Development Index rank (in all of them)
         # "=========": "PVX070",  # Impact of natural disasters: population affected
         }


def parse_file_string(filestring):
    """
    >>> parse_file_string("File 123: ABC (X, Y) Z")
    ('ABC (X, Y) Z', '')
    >>> parse_file_string("File 123: ABC (X) Y (Z)")
    ('ABC (X) Y', 'Z')
    >>> parse_file_string("File: ABC")
    ('ABC', '')
    >>> parse_file_string("File 2: A, B, 1-2")
    ('A, B, 1-2', '')
    """
    if filestring.strip()[-1] != ")":
        filestring=filestring.strip()+"()"
    if ':' in filestring:
        rhs = filestring.partition(":")[2]
    else:
        rhs = filestring
    chunks = rhs.split('(')
    indname = '('.join(chunks[:-1])
    units = chunks[-1].replace(')','')
    return indname.strip(), units.strip()


def get_metadata(socrata_id):
    url = metadata_url.format(socrata_id)
    tree = requests.get(url).json()
    return parse_metadata(socrata_id, tree)

def parse_metadata(socrata_id, tree):
    #return {'name': tree['meta']['view']['name'],
    #        'attribution': tree['meta']['view']['attribution'],
    #        'description': tree['meta']['view']['description'],
    #        'license': tree['meta']['view'].get('licenseId'),
    #        'createdat': datetime.datetime.fromtimestamp(tree['meta']['view']['createdAt']),
    #        'pubdate': datetime.datetime.fromtimestamp(tree['meta']['view']['publicationDate'])
    #       }
    raw_name = tree['meta']['view']['name']
    justname, units = parse_file_string(raw_name)

    return {"indID": lookup[socrata_id],
            "name": justname,  # TODO failing
            "units": units}
            
def get_numbers(socrata_id):
    url = data_url.format(socrata_id)
    countries = requests.get(url).json()
    return parse_numbers(socrata_id, countries)

def parse_numbers(socrata_id, countries):
    for country in countries:
        for key in country:
	    if re.match(r"_(?:19|20\d\d)", key):
	        yield {"dsID": dsID,
                       "region": country['country'],
		       "period": int(key[1:]),
		       "value": float(country[key]),
                       "indID": lookup[socrata_id],
                       "source": data_url.format(socrata_id),
                       "is_number": True}

def get_rank(socrata_id):
    url = data_url.format(socrata_id)
    countries = requests.get(url).json()
    return parse_rank(socrata_id, countries)
    

def parse_rank(socrata_id, countries):
    for country in countries:
        if 'hdi_rank' in country:
	    yield {"dsID": dsID,
		   "region": country['country'],
		   "period": 2012,  # TODO
		   "value": int(country['hdi_rank']),
		   "indID": "PSE220",
		   "source": data_url.format(socrata_id),
		   "is_number": True}
		   
                           
DataSet(**dataset).save()
maxdate=None
for socrata_code in lookup:
    ind = get_metadata(socrata_code)
    Indicator(**ind).save()
    for value in get_numbers(socrata_code):
        Value(**value).save()

print "rank"
ind = {"indID": "PSE220",
       "name": "HDI Rank",
       "units": "rank"}
Indicator(**ind).save()
for rank in get_rank("u2dx-y6wx"):
    Value(**rank).save()



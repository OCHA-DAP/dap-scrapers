import requests
import xypath
import messytables
import dl
import re
import orm

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

valuetemplate = {'dsID': 'fao-foodsec',
                 'indID': 'undernourishment',
                 'is_number': True}

indicator = {'indID': 'undernourishment',
             'name': "Prevalence of undernourishment",
             'units': 'percentage'}

dataset = {'dsID': 'fao-foodsec',
           'last_updated': None,
           'last_scraped': orm.now(),
           'name': 'FAO-Food Security'}

orm.DataSet(**dataset).save()
orm.Indicator(**indicator).save()


def niceyear(s):
    return s.partition("-")[0]+"/P3Y"

def do_file(url="http://bit.ly/14FRxGV"):
    fh = dl.grab(url)
    mts = messytables.excel.XLSTableSet(fh)
    v12 = mts['V12']
    xy = xypath.Table.from_messy(v12)
    print "...got"
    home =  xy.filter("(home)")
    years = home.fill(xypath.RIGHT)
    countries = home.fill(xypath.DOWN)
    for country, year, value in countries.junction(years):
        values = dict(valuetemplate)
        values['source'] = url
        values['region'] = country.value
        values['period'] = niceyear(year.value)  # TODO
        values['value'] = value.value
        orm.Value(**values).save()
    orm.session.commit()  
    
do_file()

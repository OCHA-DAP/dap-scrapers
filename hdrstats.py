import xypath
import messytables
#from hamcrest import equal_to, is_in
from orm import session, Value, DataSet, Indicator, send
import orm
#import re
indicator_list = """
100106
38906
68606
89006
101406
98606
98706
57506
38006
69706
103006
105906""".strip().split('\n')


"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """


def getindicator(ind="100106"):
    baseurl = 'http://hdrstats.undp.org/en/indicators/display_cf_xls_indicator.cfm?indicator_id=%s&lang=en' % ind
    value = {'dsID': 'HDRStats',
             'indID': "HDR:"+ind,
             'source': baseurl,
             'is_number': True}

    dataset = {'dsID': 'HDRStats',
               'last_updated': None,
               'last_scraped': orm.now(),
               'name': 'Human Development Indicators, UNDP'}

    send(DataSet, dataset)

    messy = messytables.excel.XLSTableSet(open("pak.xls", "rb"))
    table = xypath.Table.from_messy(list(messy.tables)[0])
    indicators = table.filter(is_in(indicator_list))
    indname = indicators.shift(x=-1)
    assert len(indname) == len(indicator_list)

    code = table.filter(equal_to('Indicator Code'))

    years = code.extend(x=1)
    for ind_cell, year_cell, value_cell in indname.junction(years):
        vdict = dict(value)
        vdict['indID'] = ind_cell.value
        vdict['period'] = year_cell.value
        vdict['value'] = value_cell.value

        indicator = {'indID':vdict['indID']}
        nameunits = re.search('(.*)\((.*)\)',vdict['indID'])
        print nameunits
        if nameunits:
            (indicator['name'], indicator['units'])=nameunits.groups()
        else:
            indicator['name']=vdict['indID']
            indicator['units']='uno'
        send(Indicator, indicator)
        send(Value, vdict)
    session.commit()

getcountry()

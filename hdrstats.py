import re
import lxml.html
import requests
import xypath
import StringIO
import messytables
#from hamcrest import equal_to, is_in
from orm import session, Value, DataSet, Indicator
import orm
import dateutil.parser
#import re
indicator_list = """
100106
38906
68606
89006
101406
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

def disasters():
    baseurl = "http://hdrstats.undp.org/en/tables/displayByRow.cfm"
    data = {"selectedCountries": "3,103,203,403,503,703,803,903,1103,1203,1303,1403,1503,1603,1703,1803,1903,2003,2103,2303,2403,2503,2603,2703,2903,3003,3103,3203,3303,3403,3503,3603,3803,3903,4003,4103,4203,4303,4403,4503,4703,4803,4903,5003,5103,5203,5303,5403,5503,5603,5703,5803,5903,6003,6103,6203,6303,6603,6703,6803,7103,7203,7303,7403,7503,7703,7903,8203,8303,8403,8503,8603,8803,8903,9003,9103,9203,9303,9403,9503,9603,9803,9903,10003,10103,10203,10303,10403,10503,10603,10703,10803,10903,11003,11103,11203,11303,11403,11503,11603,11703,11803,12103,12203,12303,12403,12503,12603,12703,12903,13003,13203,13303,13403,13503,13603,13703,13903,14003,14103,14203,14303,14403,14503,14803,14903,15003,15103,15503,15603,15703,15803,15903,16003,16103,16203,16303,16403,16603,16703,16903,17103,17203,17303,17503,17603,17803,17903,18003,18103,18203,18303,18403,18603,18703,18803,18903,19003,19103,19203,19303,19403,19503,19603,19703,19903,20003,20103,20203,20403,20503,20603,12003,20703,20803,21003,21103,21203,21303,21403,21603,21703,21803,21903,22003,22103,22203,22303,22403,22503,22603,23003,23103,23203,202,2,102,2602,302,402,602,702,502,902,802,1002,1202,1102,1402,1302,1502,1602,1702,2202,1802,2002,1902,2302,2102,2402,2502,2702,3402,3302,3502,3702,3602,3802,3902,4002,4102,",
            "selectedIndicators": "98606,",
            "selectedYears": "1960,1970,1980,1985,1990,1995,2000,2005,2006,2007,2008,2009,2010,2011,2012,",
            "language": "en",
            "displayIn": "row"}
    html = requests.post(baseurl, data=data).content
    return html, baseurl

def getindicator(ind="100106", overridefunction=None):
    if not overridefunction:
        baseurl = 'http://hdrstats.undp.org/en/indicators/display_cf_xls_indicator.cfm?indicator_id=%s&lang=en' % ind
        html = requests.get(baseurl).content
    else:
        html, baseurl = overridefunction()
    value = {'dsID': 'HDRStats',
             'indID': "HDR:"+ind,
             'source': baseurl,
             'is_number': True}

    dataset = {'dsID': 'HDRStats',
               'last_scraped': orm.now(),
               'name': 'Human Development Indicators, UNDP'}

    indicator = {'indID': "HDR:"+ind}
    hdi_indicator = {'indID': 'HDR:HDI Rank',
                     'name': 'Human Development Index rank',
                     'units': ''}
    Indicator(**hdi_indicator).save()
    DataSet(**dataset).save()
    htmlio = StringIO.StringIO(html)
    messy = messytables.html.HTMLTableSet(htmlio)
    table = xypath.Table.from_messy(list(messy.tables)[0])
    root = lxml.html.fromstring(html)

    "get odd indicator / update time"
    indicator_text = root.xpath("//h2/text()")[-1]
    print indicator_text
    try:
        indicator_split, = re.findall("(.*)\(([^\(\)]+)\)", indicator_text)
    except ValueError:
        indicator_split = [indicator_text, ""]
    indicator['name'], indicator['units'] = indicator_split
    indicator['name'] = indicator['name'].strip()
    access_text, = [x.tail.strip() for x in root.xpath("//br") if str(x.tail) != "None" and x.tail.strip()]
    access_date_raw, = re.findall('Accessed:(.*)from', access_text)
    dataset['last_updated'] = dateutil.parser.parse(access_date_raw).isoformat()
    print dataset['last_updated'], indicator['name'], "*", indicator['units']
    Indicator(**indicator).save()

    country_cell = table.filter("Country").assert_one()
    years = country_cell.fill(xypath.RIGHT).filter(lambda b: b.value != '')
    countries = country_cell.fill(xypath.DOWN)
    hdi_rank = table.filter("HDI Rank").assert_one()
    max_year = max(year.value for year in years)

    for i in countries.junction(hdi_rank):
        newvalue = dict(value)    
        newvalue['indID'] = "HDR:HDI Rank"
        newvalue['region'] = get_region(i[0])
        newvalue['value'] = i[2].value.strip()
        newvalue['period'] = 2012 # TODO Hard coded for now because year it pertains to is not clear 
        if newvalue['value'].strip() != '..':
            Value(**newvalue).save()
  
    for i in countries.junction(years):
        newvalue = dict(value)
        newvalue['region'] = get_region(i[0])
        newvalue['value'] = i[2].value.strip()
        newvalue['period'] =i[1].value.strip()
        if newvalue['value'].strip() != '..':
            Value(**newvalue).save()
        print newvalue
    session.commit()

def get_region(country):
        region_el=lxml.html.fromstring(country.properties['html'])
        try:
            link, = region_el.xpath('//a/@href')
        except ValueError:  # non-countries don't have links.
            niceregion = country.value.strip()
        else:
            niceregion, = re.findall("profiles/([^\.]*)\.html", link)
        return niceregion

#getindicator("98606", disasters)
#exit()
for ind in indicator_list:
    print ind
    getindicator(ind)



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


def getindicator(ind="100106"):
    baseurl = 'http://hdrstats.undp.org/en/indicators/display_cf_xls_indicator.cfm?indicator_id=%s&lang=en' % ind
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
    html = requests.get(baseurl).content
    htmlio = StringIO.StringIO(html)
    messy = messytables.html.HTMLTableSet(htmlio)
    table = xypath.Table.from_messy(list(messy.tables)[0])
    root = lxml.html.fromstring(html)

    "get odd indicator / update time"
    _, indicator_text = root.xpath("//h2/text()")
    print indicator_text
    try:
        indicator_split, = re.findall("(.*)\(([^\(\)]+)\)", indicator_text)
    except ValueError:
        indicator_split = [indicator_text, ""]
    indicator['name'], indicator['units'] = indicator_split
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

for ind in indicator_list:
    print ind
    getindicator(ind)



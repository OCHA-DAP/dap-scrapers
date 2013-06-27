import re
import lxml.html
import requests
import xypath
import StringIO
import messytables
#from hamcrest import equal_to, is_in
from orm import session, Value, DataSet, Indicator, send
import orm
import dateutil.parser
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
               'last_scraped': orm.now(),
               'name': 'Human Development Indicators, UNDP'}

    indicator = {'indID': "HDR:"+ind}

    send(DataSet, dataset)
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
    send(Indicator, indicator)

    country_cell = table.filter("Country").assert_one()
    years = country_cell.fill(xypath.RIGHT).filter(lambda b: b.value != '')
    countries = country_cell.fill(xypath.DOWN)
    for i in countries.junction(years):
        newvalue = dict(value)
        region_el=lxml.html.fromstring(i[0].properties['html'])
        try:
            link, = region_el.xpath('//a/@href')
        except ValueError:  # non-countries don't have links.
            newvalue['region'] = i[0].value.strip()
        else:
            newvalue['region'], = re.findall("profiles/([^\.]*)\.html", link)
        newvalue['value'] = i[2].value.strip()
        newvalue['period'] =i[1].value.strip()
        send(Value, newvalue)
    session.commit()

for ind in indicator_list:
    print ind
    getindicator(ind)

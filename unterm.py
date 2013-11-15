import datetime
import lxml.html
import requests
import xypath
import StringIO
import messytables
from orm import session, Value, DataSet, Indicator
import orm

rawformdata = '''__Click:85256981006B94CD.d6a31b1f4f85ddbb8525716a0059e321/$Body/0.44EA
%%Surrogate_col3:1
%%Surrogate_col4:1
col4:Fr
%%Surrogate_col6:1
col6:Sp
%%Surrogate_col5:1
col5:Ru
%%Surrogate_col2:1
col2:Ch
%%Surrogate_col1:1
col1:Ar
Query:
FTSearch:
%%Surrogate_Sub:1
Sub:country name
En:
Ea:
Fr:
Fa:
Sp:
Sa:
Ru:
Ra:
Ch:
Ca:
Ar:
%%Surrogate_MaxResults:1
MaxResults:2500
%%Surrogate_SearchWv:1
SearchWv:1
%%Surrogate_SearchFuzzy:1
%%Surrogate_Sort:1
Sort:1'''

indicators = {"Date of Entry into UN": xypath.RIGHT,
              "ISO Country alpha-3-code": xypath.RIGHT,
              "ISO Country alpha-2-code": xypath.RIGHT,
              "ISO Currency Code": xypath.RIGHT,
              "Short Name": xypath.RIGHT,
              "Formal Name": xypath.RIGHT,
              "Capital City": xypath.RIGHT,
              "Languages": xypath.RIGHT,
              "Currency Abbr.": xypath.DOWN}

dataset_data = {'dsID': 'unterm',
           'last_updated': "",
           'last_scraped': orm.now(),
           'name': 'unterm'}

DataSet(**dataset_data).save()


indicator_data = [{'indID': 'unterm:' + i,
                   'name': i,
                   'units': ''} for i in indicators]
for db_row in indicator_data:
    Indicator(**db_row).save()

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

value_static = {'dsID': 'unterm', 'period': '', 'is_number': False}


def country_urls():
    formdata = dict([[h.partition(':')[0], h.partition(':')[2]] for h in rawformdata.split('\n')])
    url = 'http://unterm.un.org/DGAACS/unterm.nsf/0/$searchForm?SearchView=&Seq=1'
    html = requests.post(url, data=formdata).content
    root = lxml.html.fromstring(html)
    root.make_links_absolute(url)
    return root.xpath('//a/@href')

for country in country_urls():
    print country
    html = requests.get(country).content
    root = lxml.html.fromstring(html)
    eng_tables = root.xpath('//table[following::font/text()="French"]')
    eng_text = ''.join(lxml.html.tostring(table) for table in eng_tables)
    data = {}
    for m_table in messytables.any.any_tableset(StringIO.StringIO(eng_text)).tables:
        table = xypath.Table.from_messy(m_table)
        for ind in indicators:
            target = table.filter(ind)
            if target:
                data[ind] = target.shift(indicators[ind]).value.strip()
    for item in data:
        value_data = dict(value_static)
        value_data['indID'] = 'unterm:' + item
        value_data['value'] = data[item].encode('latin1').decode('utf-8')
        value_data['region'] = data['ISO Country alpha-3-code']
        value_data['source'] = country
        value_data['period'] = datetime.datetime.now().isoformat()[:10]
        if value_data['value']:
            Value(**value_data).save()
    assert len(data) == len(indicators)
session.commit()

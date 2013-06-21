import requests
import messytables
import xypath
import StringIO
import json
from hamcrest import contains_string
import orm
import lxml.html
import re
import dateutil.parser
"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

baseurl = "http://unstats.un.org/unsd/methods/m49/m49alpha.htm"

html = requests.get(baseurl).content
fh = StringIO.StringIO(html)

root = lxml.html.fromstring(html)
updated = None
for p in root.xpath("//td[@class='content']/p/text()"):
    if re.search('\d\d\d\d', p): updated=dateutil.parser.parse(p).isoformat().split('T')[0]

dataset = {'dsID':'m49',
           'last_updated': updated,
           'last_scraped': orm.now(),
           'name':'m49'}

indicators = [{
                'indID':'m49-name',
                'name':'m49-name',
                'units':'string'
              }, {
                'indID':'m49-num',
                'name':'m49-num',
                'units':'string'
              }]

orm.send(orm.DataSet, dataset)
for x in indicators: orm.send(orm.Indicator, x)

mt = messytables.HTMLTableSet(fh)
mt_prune = [x for x in mt.tables if json.loads(x.name).get('cellpadding')=="2"]
assert len(mt_prune)==1

for messy in mt_prune:
    table = xypath.Table.from_messy(messy)
    alpha_code_header = table.filter(contains_string("ISO ALPHA-3")).assert_one()
    country_header = table.filter(contains_string("or area name")).assert_one()
    num_code_header = table.filter(contains_string("Numerical")).assert_one()
    countries = country_header.extend(y=1)
    num_code_header = countries.shift(x=-1)
    alpha_j = alpha_code_header.junction(countries)
    num_j = alpha_code_header.junction(num_code_header)
    alphas = [[x.value.strip() for x in row[1:]] for row in alpha_j]
    nums = [[x.value.strip() for x in row[1:]] for row in num_j]
    v_template = {'dsID':'m49',
                  'period': updated,
      	          'source': 'http://unstats.un.org/unsd/methods/m49/m49alpha.htm',
                  'is_number': False}
    builder = []
    for entry in alphas:
        v=dict(v_template)
        v.update({'value':entry[0], 'region':entry[1], 'indID':'m49-name'})
        orm.session.merge(orm.Value(**v))
        if 'GBR' in repr(v): print v
    for entry in nums:
        v=dict(v_template)
        v.update({'value':entry[0], 'region':entry[1], 'indID':'m49-num'})
        orm.session.merge(orm.Value(**v))
        if 'GBR' in repr(v): print v
    orm.session.commit()

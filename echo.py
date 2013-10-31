import dl
import messytables
import xypath
import orm
from header import headerheader
"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """


baseurl = 'http://ec.europa.eu/echo/files/policies/strategy/gna_2012_2013.xls'

dataset = {"dsID": "echo",
           "last_updated": None,
           "last_scraped": orm.now(),
           "name": "ECHO Europa"}


orm.DataSet(**dataset).save()

indicators = [{"indID": "gna-vi",
              "name": "GNA Vulnerability Index",
              "units": "index"},
             {"indID": "gna-ci",
              "name": "GNA Crisis Index",
              "units": "index"}]

for indicator in indicators:
    orm.Indicator(**indicator).save()

value_template = {"dsID": "echo",
                  "period": 2012,
                  "source": baseurl,
                  "is_number": True}

xls_raw = dl.grab(baseurl)
mt = messytables.excel.XLSTableSet(xls_raw).tables[0]
assert mt.name == "GNA Final Index (rank)"
xy = xypath.Table.from_messy(mt)
countries = xy.filter("ISO3").assert_one().fill(xypath.DOWN)
vuln_h = xy.filter("GNA Vulnerability Index (VI)").assert_one()
crisis_h = xy.filter("GNA Crisis Index (CI)").assert_one()

headerheader(xy.filter("ISO3").assert_one(), xypath.DOWN, xypath.RIGHT)

big = {'region': headerheader(xy.filter("ISO3").assert_one(), xypath.DOWN, xypath.RIGHT),
       'indID': {'gna-vi': vuln_h.fill(xypath.DOWN),
                     'gna-ci': crisis_h.fill(xypath.DOWN)}}

for olap_row in xypath.xyzzy.xyzzy(xy, big, valuename="value"):
    print olap_row
    full_olap = dict(value_template)
    full_olap.update(olap_row)
    orm.Value(**full_olap).save()

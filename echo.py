import dl
import messytables
import xypath
baseurl = 'http://ec.europa.eu/echo/files/policies/strategy/gna_2012_2013.xls'
xls_raw = dl.grab(baseurl)
mt = messytables.excel.XLSTableSet(xls_raw).tables[0]
assert mt.name == "GNA Final Index (rank)"
xy = xypath.Table.from_messy(mt)
countries = xy.filter("ISO3").assert_one().fill(xypath.DOWN)
vuln_h = xy.filter("GNA Vulnerability Index (VI)").assert_one()
crisis_h = xy.filter("GNA Crisis Index (CI)").assert_one()

big = {'region': xy.filter("ISO3").assert_one().headerheader(xypath.DOWN, xypath.RIGHT),
       'indicator': {'vuln': vuln_h.fill(xypath.DOWN),
                     'crisis': crisis_h.fill(xypath.DOWN)}}

for i in xy.xyzzy(big):
    print i


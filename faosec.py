import re
import xypath
import messytables
import dl
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
    return s.partition("-")[0] + "/P3Y"

def discover_table(all_tables):
    found = []
    for table in all_tables.tables:
        t = xypath.Table.from_messy(table)
        if t.filter(re.compile(".*Prevalence of [Uu]ndernourishment.*")).filter(lambda cell: cell.y==0) \
            and t.filter(lambda cell: cell.x==7 and cell.value):
            found.append(t)
    if len(found) != 1:
        print found
        print len(found), "found"
        raise RuntimeError, "expected 1 found {}".format(len(found))
    return found[0]



def do_file(url="http://bit.ly/14FRxGV"):
    fh = dl.grab(url)
    mts = messytables.excel.XLSTableSet(fh)
    xy = discover_table(mts)
    print "...got"
    home = xy.filter("(home)")
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

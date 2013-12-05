import datetime
import itertools
import dl
import xypath
import messytables
import orm

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

url = 'http://cred01.epid.ucl.ac.be:5317/?after=&before=&agg1=iso&agg2=year&dl=true'
raw = dl.grab(url)
m_tables = messytables.any.any_tableset(raw)
mt, = m_tables.tables
table = xypath.Table.from_messy(mt)

def doit(targets, names, year):
    # country_cells: we used to assert_one(), but sometimes there's two!
    country_cells = table.filter('iso').fill(xypath.DOWN)
    country_cells = country_cells - country_cells.filter('iso')  # remove other
    if not country_cells: print "no countries"
    country_year_filter = country_cells.filter(lambda b: b.shift(xypath.RIGHT).value == year)
    if not country_year_filter: print "no countries for ", year
    target_cells = table.filter(lambda b: b.value in targets)
    if not target_cells: print "didn't find ", targets

    value = {'dsID': 'emdat',
             'period': "%s/P1Y" % (year),
             'source': url,
             'is_number': True}

    dataset = {'dsID': 'emdat',
               'last_updated': None,
               'last_scraped': orm.now(),
               'name': 'EM-DAT'}
    orm.DataSet(**dataset).save()

    for i, t in enumerate(targets):
        indicator = {'indID': "emdat:%s" % t,
                     'name': names[i],
                     'units': 'uno'}
        if t == 'total_dam':
            indicator['units'] = ",000$ USD"
        orm.Indicator(**indicator).save()
    for cname, one_country_cells in itertools.groupby(country_year_filter, lambda b: b.value):
        value['region'] = cname
        one_country_bag = xypath.Bag.from_list(one_country_cells, name=cname)
        for target_cell in target_cells:
            j = one_country_bag.junction(target_cell)
            value['indID'] = 'emdat:%s' % target_cell.value
            value['value'] = sum(int(x[2].value) for x in j)
            orm.Value(**value).save()
            print value
    orm.session.commit()

targets = 'no_disasters no_killed no_injured no_affected no_homeless total_affected total_dam'.split(' ')
names = ["Number of disasters", "People killed in disasters",
         "People injured in disasters", "People affected by disasters",
         "People made homeless by disasters",
         "Total number of people affected by disasters",
         "Total cost of damage done by disasters"]

for year in range(1900, datetime.datetime.now().year+1):
    print year
    doit(targets, names, str(year))

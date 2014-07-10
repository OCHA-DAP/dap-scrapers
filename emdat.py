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

url = "http://www.emdat.be/advanced_search/php/downloadCsv.php?continent=&region=&country_name={}&dis_group=Complex+Disasters%27%2C%27Natural%27%2C%27Technological&dis_subgroup=&dis_type=&dis_subtype=&aggreg1=year&start_year=1900&end_year=2014&aggreg2="

def country_list():
    for value in orm.session.query(orm.Value).filter(orm.Value.indID == "CG060").all():
        yield value.region

def doit():
    # country_cells: we used to assert_one(), but sometimes there's two!

    dataset = {'dsID': 'emdat',
               'last_updated': None,
               'last_scraped': orm.now(),
               'name': 'EM-DAT'}
    orm.DataSet(**dataset).save()

    for i, t in enumerate(targets):
        indicator = {'indID': "emdat:%s" % t,
                     'name': names[i],
                     'units': 'uno'}
        if t == 'total_damage':
            indicator['units'] = ",000$ USD"
        orm.Indicator(**indicator).save()
    
    for country in country_list():  # TODO country_list
        print country
        raw = dl.grab(url.format(country))
        m_tables = messytables.any.any_tableset(raw)
        mt, = m_tables.tables
        table = xypath.Table.from_messy(mt)
        yr = table.filter('year').assert_one()
        years = yr.fill(xypath.DOWN)
        cats = yr.fill(xypath.RIGHT)
        for year, cat, value in years.junction(cats):
            print year, cat, value
            value = {'dsID': 'emdat',
                     'region': country,
                     'indID': 'emdat:{}'.format(cat.value),
                     'period': '{}/P1Y'.format(year.value),
                     'value': value.value,
                     'source': url,
                     'is_number': True}
            orm.Value(**value).save()
            print value
    orm.session.commit()

targets = 'occurrence deaths injured affected homeless total_affected total_damage'.split(' ')
names = ["Number of disasters", "People killed in disasters",
         "People injured in disasters", "People affected by disasters",
         "People made homeless by disasters",
         "Total number of people affected by disasters",
         "Total cost of damage done by disasters"]

doit()

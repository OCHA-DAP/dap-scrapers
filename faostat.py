import requests
import xypath
import messytables
import dl
import re
import orm

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

valuetemplate = {'dsID': 'faostat3',
                 'indID': 'calories_per_person',
                 'is_number': True}

indicator = {'indID': 'calories_per_person',
             'name': "Per capita food supply",
             'units': 'kcal/capita/day'}

dataset = {'dsID': 'faostat3',
           'last_updated': None,
           'last_scraped': orm.now(),
           'name': 'FAOstat'}

orm.DataSet(**dataset).save()
orm.Indicator(**indicator).save()

def get_zip_urls(basename="http://faostat.fao.org/Portals/_Faostat/Downloads/zip_files/%s"):
    jsonurl = "http://faostat3.fao.org/wds/rest/bulkdownloads/faostat2/CC/E"
    ajax = requests.get(jsonurl).json()
    return [basename % i[2] for i in ajax if '+' not in i[3]]


def do_zip(url):
    print url
    fh = dl.grab(url)
    mt, = list(messytables.zip.ZIPTableSet(fh).tables)
    fh = None
    xy = xypath.Table.from_messy(mt)
    mt = None
    print "...got"
    headers = xy.filter(lambda c: c.y == 0)
    country = headers.filter("Country").assert_one()
    items = headers.filter("Item").assert_one().fill(xypath.DOWN).filter("Grand Total + (Total)")
    elements = headers.filter("Element").assert_one().fill(xypath.DOWN).filter("Food supply (kcal/capita/day)")
    filtered_items = items.select_other(lambda a, b: a.y == b.y, elements)

    years = country.fill(xypath.RIGHT).filter(re.compile("Y\d\d\d\d$"))

    for i in filtered_items:
        values = dict(valuetemplate)
        values['source'] = url
        countrycodecell, = i.junction(country)
        values['region'] = countrycodecell[2].value
        year_junction = i.junction(years)
        for _, period, value in year_junction:
            values['period'] = period.value.replace("Y", "")
            values['value'] = value.value
            orm.Value(**values).save()
    orm.session.commit()

for i in get_zip_urls():
    do_zip(i)

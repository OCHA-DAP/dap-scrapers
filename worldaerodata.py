import dshelpers
import lxml.html
import orm

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

dataset = {'dsID': 'worldaerodata',
           'last_updated': None,
           'last_scraped': orm.now(),
           'name': 'worldaerodata.com'}

orm.DataSet(**dataset).save()

indicator = {'indID': 'airports',
             'name': 'Number of airports',
             'units': 'count'}

orm.Indicator(**indicator).save()

value_template = {"dsID": "worldaerodata",
                  "indID": "airports",
                  "period": None,
                  "source": "http://worldaerodata.com/countries/",
                  "is_number": True}


def do_airports(baseurl):
    html = dshelpers.request_url(baseurl).content
    root = lxml.html.fromstring(html)
    root.make_links_absolute(baseurl)
    country_links = root.xpath("//li/a")

    for country in country_links:
        country_name = country.text
        country_url = country.attrib['href']
        country_html = dshelpers.request_url(country_url).content
        country_root = lxml.html.fromstring(country_html)
        airports = len(country_root.xpath("//table//table//a"))
        yield country_name, airports

world_url = "http://worldaerodata.com/countries/"
world = dict(do_airports(world_url))
del world['United States']
us_url = "http://worldaerodata.com/US/"
us = list(do_airports(us_url))
world['United States'] = sum([x[1] for x in us])

for country in world:
    value = dict(value_template)
    value['region'] = country
    value['value'] = world[country]
    orm.Value(**value).save()

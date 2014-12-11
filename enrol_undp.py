import logging
import dl
import messytables
import xypath
import re
import orm
import requests
import lxml.html

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

log = logging.getLogger("unicef")
log.addHandler(logging.StreamHandler())
log.addHandler(logging.FileHandler("unicef.log"))
log.level = logging.WARN

dataset = {"dsID": "enrol-undp",
           "last_updated": None,
           "last_scraped": orm.now(),
           "name": "UNDP - Education"
          }

indicator = {"indID": "PVE120",
             "name": "Combined gross enrolment in education (both sexes)",
             "units": "Percentage"
            }

output_template = {"dsID": "enrol-undp",
                   "is_number": True,
                   "indID": "PVE120",
}


def getstats():
    url = 'http://hdr.undp.org/en/content/combined-gross-enrolment-education-both-sexes'
    handle = dl.grab(url)
    mts = messytables.any.any_tableset(handle)
    saves = 0
    mt = mts.tables[0]
    table = xypath.Table.from_messy(mt)

    pivot, = table.filter(lambda c: 'Country' in c.value)
    years = pivot.fill(xypath.RIGHT)
    countries = pivot.fill(xypath.DOWN)
    for year, country, value in years.junction(countries):
        output = dict(output_template)
        output['source'] = url
        output['region'] = country.value.strip()
        output['value'] = value.value.strip()
        orm.Value(**output).save()
        saves = saves + 1
    assert saves


if __name__ == "__main__":
    orm.DataSet(**dataset).save()
    orm.Indicator(**indicator).save()
    getstats()


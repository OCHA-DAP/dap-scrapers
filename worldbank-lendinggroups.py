import re
import requests
import lxml.html
import orm
baseurl = "http://data.worldbank.org/about/country-classifications/country-and-lending-groups"
html = requests.get(baseurl).content
root = lxml.html.fromstring(html)

divs = root.xpath("//div[@class='node-body']/div[.//*[contains(text(),'economies')]]")
builder = []
for div in divs:
    econ_raw, = div.xpath(".//p/b/text()")
    econ, = re.findall("(.*) economies", econ_raw)
    els = div.xpath(".//following::table[1]//td")
    countries = [x.text_content().strip() for x in els]
    builder.extend([{'value': econ, 'region':country} for country in countries])
print builder

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

dsID = "worldbank-lending-groups"
indID = "worldbank-income"

dataset = {"dsID": dsID,
           "last_updated": '',
           "last_scraped": orm.now(),
           "name": "World Bank Lending Groups"}
orm.DataSet(**dataset).save()

indicator = {"indID": indID,
             "name": "Income Category",
             "units": ''}
orm.Indicator(**indicator).save()

value_template = {"dsID": dsID,
                  "indID": indID,
                  "period": '',
                  "source": baseurl,
                  "is_number": False}

for item in builder:
    value = dict(value_template)
    value.update(item)
    try:
        orm.Value(**value).save()
    except:
        print value
        raise

orm.session.commit()

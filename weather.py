import requests
import orm
"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
"""

def accuweather():
    baseindexurl = "http://www.accuweather.com/ajax-service/getcountrylist?region=%s&languageID=1"
    baseleafurl = "http://www.accuweather.com/en/%s/%s-weather"
    regions = "afr ant arc asi cac eur mea nam ocn sam".split(" ")

    for reg in regions:
        j = requests.get(baseindexurl%reg).json()
        for country in j['Countries']:
            yield {'region':country['Code'], 'value':baseleafurl%(country['Code'], country['OfficialName'])}

print list(accuweather())

orm.DataSet(dsID="accuweather",
            last_updated=None,
            last_scraped=orm.now(),
            name="Accuweather").save()

orm.Indicator(indID="accuweather_url",
              name="AccuWeather URL",
              units="").save()

valuetemplate = {'dsID': 'accuweather',
                 'indID': 'accuweather_url',
                 'period': None,
                 'source': 'http://www.accuweather.com'}

for datarow in accuweather():
    olap_row = dict(valuetemplate)
    olap_row.update(datarow)
    orm.Value(**olap_row).save()


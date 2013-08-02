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

dataset = { "dsID": "unicef-infobycountry",
            "last_updated": None,
            "last_scraped": orm.now(),
            "name": "UNICEF info by country"
          }

value_template = {"dsID": "unicef-infobycountry",
                  "is_number": True}

def split_ind(indtext):
    """
    >>> split_ind("Crude death rate, 1970")
    {'units': '', 'indID': 'Crude death rate', 'period': '1970'}
    >>> split_ind("Public spending as a % of GDP (2007-2010*) allocated to: military")
    {'units': '', 'indID': 'Public spending as a % of GDP allocated to: military', 'period': '2007/2010'}
    >>> split_ind("Population (thousands) 2011, total")
    {'units': 'thousands', 'indID': 'Population , total', 'period': '2011'}
    """

    """
    1) extract years
    2) extract bracketed text as units
    3) rest as ind name
    """
    indtext=indtext.replace("*","")
    try:
        start, y1, y2, end = re.search("(.*?)(\d\d\d\d)-?(\d\d\d\d)?(.*)", indtext).groups()
    except:
        print "Couldn't parse %r"% indtext
        return {'indID': indtext, 'period':'', 'units':''}
    if y2:
        period = '/'.join((y1,y2))
    else:
        period = y1
    rest = start + end
    unit_search = re.search("(.*)\((.*)\)(.*)", rest)
    if unit_search:
        preunit, unit, postunit = unit_search.groups()
        ind = preunit.strip()+" "+postunit.strip()
    else:
        unit = ''
        ind = rest
    ind=ind.strip(" ,")
    return {'indID': ind, 'period': period, 'units': unit}
    

def getstats(url, country="PLACEHOLDER"):
    handle = dl.grab(url)
    mts = messytables.any.any_tableset(handle)
    saves = 0
    for mt in mts.tables:
        table = xypath.Table.from_messy(mt)
        inds = table.filter(lambda b: b.x==0 and "EPI" in b.value)
        if not inds: 
            continue
        assert len(inds)==1
        top, = table.filter(lambda b: 'to the top' in b.value)
        value, = inds.junction(top)
        for ind in inds:
            split = split_ind(ind.value)
            values_tosave = dict(value_template)
            values_tosave['source'] = url
            values_tosave['region'] = country
            values_tosave['value'] = value[2].value
            indicator = {'indID': split['indID'], 'name': split['indID'], 'units': split['units']}
            orm.Indicator(**indicator).save()
            values_tosave['indID']= split['indID']
            orm.Value(**values_tosave).save()
            saves = saves + 1
    if saves != 1:
        print "huh, %d saves for %r"%(saves, url)

   
def countrylist():
    baseurl = 'http://www.unicef.org/infobycountry'
    html = requests.get(baseurl).content
    root = lxml.html.fromstring(html)
    root.make_links_absolute(baseurl)
    mostcountries = root.xpath("//div[@class='contentrow' or @class='contentrow_last']//a")
    for country in mostcountries:
        url = country.attrib['href']
        if 'bvi.html' in country:
            continue
        html = requests.get(url).content
        root = lxml.html.fromstring(html)
        root.make_links_absolute(baseurl)
        try:
            link, = root.xpath('//a[normalize-space(text())="Statistics"]/@href')
        except:
            log.warn("No stats found for %r"%url)
            continue
        
        yield link, country.text_content()
        
if __name__=="__main__":
    orm.DataSet(**dataset).save()
    for link, country in countrylist():
        print repr([link, country])
        getstats(link, country)


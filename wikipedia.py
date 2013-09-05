import codecs
import sys
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

from wikihead import headings
headers = u"""
Geography
Climate
Economy
Transport
Education
Demographics
Religion
""".strip().lower().split('\n')

import orm
"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
      Indicator: indID, name, units
         """

dataset = {'dsID': 'wikipedia',
           'last_updated': None,  # TODO
           'last_scraped': orm.now(),
           'name': 'Wikipedia'}

orm.DataSet(**dataset).save()

for h in headers:
    indicator = {'indID': 'wikipedia:'+h,
                 'name': 'Wikipedia: '+h,
                 'units': 'url'}
    orm.Indicator(**indicator).save()

value_template = {'dsID': 'wikipedia',
                  'period': None,
                  'is_number': False}


def exact_match(level, header, country):
    matches = [x.lower() for x in country[level] if x == header]
    if len(matches) > 1:
        print matches, "\n\n\n\n"
    if matches:
        return matches[0]


def partial_match(level, header, country):
    matches = []
    for cand_header in country[level]:
        if header in cand_header.lower():
            matches.append(cand_header)
    if len(matches) > 1:
        print matches, "\n\n\n\n"
    if matches:
        return matches[0]


def best_match(header, country):
    return exact_match(2, header, country) or \
        exact_match(3, header, country) or \
        partial_match(2, header, country) or \
        partial_match(3, header, country)


wikibase = "http://en.wikipedia.org/wiki/%s#%s"
headinglist = headings()
for country in headinglist:
    matches = [best_match(header, headinglist[country]) for header in headers]
    d_matches = dict(zip(headers, matches))
    for head in d_matches:
        value = dict(value_template)
        value['region'] = country
        value['indID'] = head
        value['source'] = wikibase % (country, head)
        value['value'] = value['source']
        print value
        orm.Value(**value).save()

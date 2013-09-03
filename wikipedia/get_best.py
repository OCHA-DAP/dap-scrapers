import codecs
import sys
from get_headings import headings
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
headers = u"Geography, Climate, Economy, Transport, Education, Demographics, Religion".lower().split(', ')

def exact_match(level, header, country):
    matches = [x.lower() for x in country[level] if x==header]
    if len(matches)>1: print matches, "\n\n\n\n"
    if matches: return matches[0]

def partial_match(level, header, country):
    matches = []
    for cand_header in country[level]:
        if header in cand_header.lower():
            matches.append(cand_header)
    if len(matches)>1: print matches, "\n\n\n\n"
    if matches: return matches[0]

def best_match(header, country):
    return exact_match(2, header, country) or \
           exact_match(3, header, country) or \
           partial_match(2, header, country) or \
           partial_match(3, header, country)

headinglist = headings()
for country in headinglist:
    print repr(country), [best_match(header, headinglist[country]) for header in headers]

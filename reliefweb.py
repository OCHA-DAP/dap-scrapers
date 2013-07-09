import re
import requests
import lxml.html

# unfinished! others undiscovered

def sitrep(root):
    block = root.get_element_by_id('facetapi-facet-search-apireports-block-field-content-format')
    text_chunk, = block.xpath(".//a[contains(text(), 'Situation Report')]/text()")
    pair = re.findall("(.*) \((.*)\)", text_chunk)[0]
    return {'indID':pair[0], 'value':int(pair[1])}


html = requests.get('http://reliefweb.int/country/gbr').content
assert "Situation Report" in html
root = lxml.html.fromstring(html)
print sitrep(root)

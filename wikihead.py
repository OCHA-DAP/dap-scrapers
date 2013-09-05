import codecs
import sys
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
import lxml.etree, lxml.html
import requests
from collections import Counter
import urllib

def headings():
    headers = "Geography, Climate, Economy, Transport, Education, Demographics, Religion".lower().split(', ')

    all_cats = Counter()

    xml = requests.get("http://en.wikipedia.org/wiki/Special:Export/List_of_sovereign_states").content
    root = lxml.etree.fromstring(xml)
    text = root.xpath("//*[local-name()='text']")[0].text

    wrapped_text = u"".join([u"<html>", text, u"</html>"])
    w_root = lxml.html.fromstring(wrapped_text)
    countries = w_root.xpath("//span/@id")

    builder = {}
    for country in countries:
        html = requests.get("http://en.wikipedia.org/wiki/%s"%country).content
        root = lxml.html.fromstring(html)
        cats = {2: root.xpath("//div[@id='mw-content-text']/h2/span/@id"),
                3: root.xpath("//div[@id='mw-content-text']/h3/span/@id")}
        if cats == {2:[], 3:[]}:
            print repr(country)
            raise RuntimeError()
        builder[country] = cats
    return builder

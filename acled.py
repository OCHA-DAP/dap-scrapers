import requests
import lxml.html
import dl
import messytables
import xypath

def parsesheet(url):
    rowset = messytables.excel.XLSTableSet(dl.grab(url)).tables[0]
    for row in rowset:
        print row
        return
        


def geturls(baseurl = 'http://www.acleddata.com/data/types-and-groups/'):
    html = requests.get(baseurl).content
    root = lxml.html.fromstring(html)
    root.make_links_absolute(baseurl)
    return root.xpath("//div[@id='content']//article//a[contains(text(),'xls')]/@href")

for url in geturls():
    parsesheet(url)

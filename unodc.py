import messytables
import xypath
import dl
import re
import orm
"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

SHEET_URL = "http://www.unodc.org/documents/data-and-analysis/statistics/crime/CTS_Sexual_violence.xls"

REGION_HEADER_VALUE = "Country/territory"

DSID = "unodc"
INDID = "TT029"

IND_NAME = "Total sexual violence at the national level, number of police-recorded offences"

def save_dataset():
    dataset = {"dsID": DSID,
               "last_updated": None,
               "last_scraped": orm.now(),
               "name": "United Nations Office on Drugs and Crime"}
    orm.DataSet(**dataset).save()

def save_indicator():
    indicator = {"indID": INDID,
                 "name": IND_NAME,
                 "units": "count"}
    orm.Indicator(**indicator).save()

def main():
    save_dataset()
    save_indicator()
    raw = dl.grab(SHEET_URL)
    mtables = messytables.any.any_tableset(raw)
    table = xypath.Table.from_messy(mtables.tables[0])
    table.filter(IND_NAME).assert_one()  # we have the right table
    region_header = table.filter(REGION_HEADER_VALUE).assert_one()
    regions = region_header.fill(xypath.DOWN)
    years = region_header.fill(xypath.RIGHT, stop_before=lambda c: c.value == '')
    assert len(years)<15  # left side.
    for region_cell, year_cell, value_cell in regions.junction(years):
        value = {"dsID": DSID,
                 'region': region_cell.value,
                 'indID': INDID,
                 'source': SHEET_URL,
                 'is_number': True,
                 'period': year_cell.value,
                 'value': value_cell.value
                }
        orm.Value(**value).save()
    orm.session.commit()

if __name__=="__main__":
    main()

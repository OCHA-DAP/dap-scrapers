import messytables
import xypath
import dl
import re
import orm
"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

spreadsheets = """
http://esa.un.org/unpd/wpp/Excel-Data/EXCEL_FILES/3_Mortality/WPP2012_MORT_F02_CRUDE_DEATH_RATE.XLS
http://esa.un.org/unpd/wpp/Excel-Data/EXCEL_FILES/3_Mortality/WPP2012_MORT_F03_1_DEATHS_BOTH_SEXES.XLS
http://esa.un.org/unpd/wpp/Excel-Data/EXCEL_FILES/3_Mortality/WPP2012_MORT_F01_2_Q5_BOTH_SEXES.XLS
http://esa.un.org/unpd/wpp/Excel-Data/EXCEL_FILES/3_Mortality/WPP2012_MORT_F07_1_LIFE_EXPECTANCY_0_BOTH_SEXES.XLS
http://esa.un.org/unpd/wpp/Excel-Data/EXCEL_FILES/3_Mortality/WPP2012_MORT_F01_1_IMR_BOTH_SEXES.XLS
http://esa.un.org/unpd/wup/CD-ROM/WUP2011-F02-Proportion_Urban.xls
http://esa.un.org/unpd/wpp/Excel-Data/EXCEL_FILES/1_Population/WPP2012_POP_F01_1_TOTAL_POPULATION_BOTH_SEXES.XLS
http://esa.un.org/unpd/wpp/Excel-Data/EXCEL_FILES/1_Population/WPP2012_POP_F02_POPULATION_GROWTH_RATE.XLS
http://esa.un.org/unpd/wpp/Excel-Data/EXCEL_FILES/1_Population/WPP2012_POP_F05_MEDIAN_AGE.XLS
http://esa.un.org/unpd/wpp/Excel-Data/EXCEL_FILES/1_Population/WPP2012_POP_F06_POPULATION_DENSITY.XLS
""".strip().split('\n')

def parse_file_string(filestring):
    """
    >>> parse_file_string("File 123: ABC (X, Y) Z")
    ('ABC (X, Y) Z', '')
    >>> parse_file_string("File 123: ABC (X) Y (Z)")
    ('ABC (X) Y', 'Z')
    >>> parse_file_string("File: ABC")
    ('ABC', '')
    >>> parse_file_string("File 2: A, B, 1-2")
    ('A, B, 1-2', '')
    """
    if filestring.strip()[-1] != ")":
        filestring=filestring.strip()+"()"
    rhs = filestring.partition(":")[2]
    chunks = rhs.split('(')
    indname = '('.join(chunks[:-1])
    units = chunks[-1].replace(')','')
    return indname.strip(), units.strip()
    
   
def main():
  for sheet in spreadsheets:
    print sheet
    shortname = sheet.split('/')[-1].split('.')[0]
    dsID = 'esa-unpd-' + shortname.replace('_', '-').split('-')[0]
    year_text, = re.findall('\d{4}', dsID)
    dataset = {"dsID": dsID,
               "last_updated": year_text,
               "last_scraped": orm.now(),
               "name": "esa-unpd"}

    orm.DataSet(**dataset).save()
    indicator = {"indID": shortname,
                 "name": shortname,
                 "units": ''
                }
    # we replace the indicator name, so not saving now.
    # orm.Indicator(**indicator).save()
    value_template = {"dsID": dsID,
                      "is_number": True,
                      "source": sheet}

    raw = dl.grab(sheet)
    mtables = messytables.any.any_tableset(raw)
    names = [x.name for x in mtables.tables]
    if 'ESTIMATES' in names:
        mt = mtables['ESTIMATES']
    else:
        mt = mtables['PROPORTION-URBAN']
    table = xypath.Table.from_messy(mt)

    filestring = table.filter(re.compile("File[^:]*:.*")).assert_one().value
    indicator['name'], indicator['units'] = parse_file_string(filestring)
    print indicator['name']
    orm.Indicator(**indicator).save()

    region_header = table.filter(re.compile("Major area, region, country or area.*")).assert_one()
    ccode_header = table.filter(re.compile("Country.code")).assert_one()
    regions = region_header.fill(xypath.DOWN)
    years = ccode_header.fill(xypath.RIGHT)
    for region_cell, year_cell, value_cell in regions.junction(years):
        value = dict(value_template)
        value['indID'] = indicator['indID']
        value['region'] = region_cell.value
        year_value = year_cell.value
        if isinstance(year_value, basestring) and '-' in year_value:
            year1, _, year2 = year_value.partition('-')
            year_count = int(year2) - int(year1)
            assert year_count == 5
            year_value = "%s/P%dY" % (year1, year_count)
        value['period'] = year_value
        value['value'] = value_cell.value
        orm.Value(**value).save()
        #print value
  orm.session.commit()

if __name__=="__main__":
    main()

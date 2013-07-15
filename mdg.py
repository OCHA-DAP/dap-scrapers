import re
import messytables
import xypath
import dl
import orm

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

indicators = ["566",  # population undernourished, percentage
              "563",  # inverse of infants lacking measles immunization
              "665",  # improved water
              "668",  # improved sanitation
              "553",  # maternal mortality
              "561",  # under 5 mortality
              "589",  # primary education ratio
              "559",  # severely underweight
              "755",  # }
              "756",  # } telecoms x3
              "605",  # }
             ]

dataset = {"dsID": "mdgs",
           "last_updated": None,
           "last_scraped": orm.now(),
           "name": "Millennium Development Goals"}

value_template = {"dsID": "mdgs",
                  "is_number": True}

def do_indicator(ind="566"):
    baseurl="http://mdgs.un.org/unsd/mdg/Handlers/ExportHandler.ashx?Type=Csv&Series=%s"
    url = baseurl % ind
    value_template['source'] = url
    handle = dl.grab(url)
    mt, = messytables.any.any_tableset(handle).tables
    table = xypath.Table.from_messy(mt)
    country_anchor = table.filter("Country").assert_one()
    years = country_anchor.fill(xypath.RIGHT).filter(re.compile("\d\d\d\d"))
    countries = country_anchor.fill(xypath.DOWN)
    indicator = table.filter("Series").shift(xypath.DOWN).value
    if ',' in indicator:
        i_name = ', '.join(indicator.split(', ')[:-1])
        i_unit = indicator.split(', ')[-1]
    else:
        i_name = indicator
        i_unit = ''
    value_template['indID']=indicator
    indicator = {'indID': indicator,
                 'name': i_name,
                 'units': i_unit}
    orm.Indicator(**indicator).save()
    # countries also gets some rubbish, but junction will ignore it.
    for c_cell, y_cell, v_cell in countries.junction(years):
        value = dict(value_template)
        value['region']=c_cell.value
        value['period']=y_cell.value
        value['value']=v_cell.value
        orm.Value(**value).save()


orm.DataSet(**dataset).save()
for ind in indicators:
    do_indicator(ind)
orm.session.commit()    

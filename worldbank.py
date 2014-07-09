import xypath
import messytables
from hamcrest import equal_to, is_in
from orm import session, Value, DataSet, Indicator
import orm
import re
import time
import dl

indicator_list = """
SP.POP.TOTL
SP.POP.DPND.OL
SP.POP.DPND.YG
SH.DTH.IMRT
SM.POP.TOTL.ZS
AG.LND.TOTL.K2
SP.POP.GROW
EN.POP.DNST
EN.URB.MCTY
NY.GNP.MKTP.CD
SI.POV.DDAY
SP.DYN.AMRT.FE
SP.DYN.AMRT.MA
LP.LPI.OVRL.XQ
IS.ROD.TOTL.KM
IS.RRS.TOTL.KM
NY.GNP.MKTP.PP.CD
NY.GDP.PCAP.PP.CD
NY.GNP.PCAP.PP.CD
DT.ODA.ODAT.PC.ZS
IT.CEL.SETS.P2
IT.NET.USER.P2
NE.CON.PRVT.PP.KD
EG.ELC.ACCS.ZS
SI.POV.GINI
FP.CPI.TOTL.ZG
SN.ITK.DEFC.ZS
IT.MLT.MAIN.P2
NY.GDP.PCAP.PP.KD
SH.DYN.AIDS.ZS
SL.EMP.INSV.FE.ZS
""".strip().split('\n')


"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """


dataset = {'dsID': 'World Bank',
           'last_updated': None,
           'last_scraped': orm.now(),
           'name': 'World Bank'}

DataSet(**dataset).save()


def getcountrylist():
    for value in session.query(Value).filter(Value.indID == "CG060").all():
        yield value.region


def getcountry(threeletter="PAK"):
    print threeletter
    baseurl = "http://api.worldbank.org/v2/en/country/{}?downloadformat=excel"
    value = {'dsID': 'World Bank',
             'region': threeletter,
             'source': baseurl.format(threeletter.lower()),
             'is_number': True}

    while True:
        fh = dl.grab(baseurl.format(threeletter.lower()), [404])
        if not fh:
            return
        try:
            messy = messytables.excel.XLSTableSet(fh)
            break  # success!
        except messytables.error.ReadError, e:
            print e
            return

    table = xypath.Table.from_messy(list(messy.tables)[0])
    indicators = table.filter(is_in(indicator_list))
    indname = indicators.shift(x=-1)
    if not len(indname) == len(indicator_list):
        print "missing indicators", [x.value for x in indname]

    code = table.filter(equal_to('Indicator Code'))

    years = code.fill(xypath.RIGHT)
    junction = indname.junction(years)
    for ind_cell, year_cell, value_cell in junction:
        vdict = dict(value)
        vdict['indID'] = ind_cell.value
        vdict['period'] = year_cell.value
        vdict['value'] = value_cell.value

        indicator = {'indID': vdict['indID']}
        nameunits = re.search('(.*)\((.*)\)', vdict['indID'])
        if nameunits:
            (indicator['name'], indicator['units']) = nameunits.groups()
        else:
            indicator['name'] = vdict['indID']
            indicator['units'] = 'uno'
        Indicator(**indicator).save()
        v = Value(**vdict)
        if not v.is_blank():
            v.save()
    print len(session.query(Value).filter(Value.dsID == 'World Bank').all())
    session.commit()

for country in getcountrylist():
    try:
        getcountry(country)
    except Exception, e:
        print country, e
        raise

import xypath
import messytables
from hamcrest import equal_to, is_in
from orm import session, Value, DataSet, Indicator, send
import datetime
import re
indicator_list = """
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
SI.POV.GINI""".strip().split('\n')


"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

def getcountry(threeletter="PAK"):
    print threeletter
    value = {'dsID':'World Bank',
             'region':threeletter,
             'source':'World Bank:' + threeletter,
             'is_number':True}
    
    dataset = {'dsID':'World Bank',
               'last_updated':None,
               'last_scraped':datetime.datetime.now().isoformat(),
               'name': 'World Bank'}
    
    send(DataSet, dataset)

    messy = messytables.excel.XLSTableSet(open("pak.xls", "rb"))
    table = xypath.Table.from_messy(list(messy.tables)[0])
    indicators = table.filter(is_in(indicator_list))
    indname = indicators.shift(x=-1)
    assert len(indname) == len(indicator_list)

    code = table.filter(equal_to('Indicator Code'))

    years = code.extend(x=1)
    for ind_cell, year_cell, value_cell in indname.junction(years):
        vdict = dict(value)
        vdict['indID'] = ind_cell.value
        vdict['period'] = year_cell.value
        vdict['value'] = value_cell.value

        indicator = {'indID':vdict['indID']}
        nameunits = re.search('(.*)\((.*)\)',vdict['indID'])
        if nameunits:
            (indicator['name'], indicator['units'])=nameunits.groups()
        else:
            indicator['name']=vdict['indID']
            indicator['units']='uno'
        send(Indicator, indicator)
        send(Value, vdict)
    session.commit()

getcountry()

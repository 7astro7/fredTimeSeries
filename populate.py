
from fredDB.fredDBTable import fredDBTable

seriesToCreate = {'gdp': "A191RL1Q225SBEA", 
    'federal_debt': "GFDEGDQ188S", 'shm': "SAHMREALTIME", 
    'reserves': "TRESEGUSM052N", 'scHP': "CSUSHPISA", 
    'reit': "WILLREITIND", 'fed_reserves': "RESBALNSW", 
    }

for seriesName in seriesToCreate.keys():
    fredDBTable(seriesName = seriesToCreate[seriesName]).populateTable()



from fredDB.fredDBTable import fredDBTable

seriesToCreate = {'gdp': "A191RL1Q225SBEA", 
    'federal_debt': "GFDEBTN", 'shm': "SAHMREALTIME", 
    'reserves': "TRESEGUSM052N", 'scHP': "CSUSHPISA", 
    'reit': "WILLREITIND"}

for seriesName in seriesToCreate.keys():
    fredDBTable(seriesName = seriesToCreate[seriesName]).populateTable()


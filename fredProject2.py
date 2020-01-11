import pandas as pd
import requests
from statsmodels.tsa.stattools import adfuller
import scipy
import matplotlib.pyplot as plt
import numpy as np

sch, tmkt = pd.read_csv('CSUSHPISA.csv'), pd.read_csv('WILL5000INDFC.csv')
rt, gld = pd.read_csv('WILLREITIND.csv'), pd.read_csv('GOLDPMGBD228NLBM.csv')

def assignDates(df, colIndex):
    '''assign date type'''
    x = pd.to_datetime(df.iloc[:,colIndex], yearfirst = True)
    df.iloc[:,colIndex] = pd.DataFrame(x)
    return df

rt, sch = assignDates(rt, 0), assignDates(sch, 0)
gld, tmkt = assignDates(gld, 0), assignDates(tmkt, 0)

def checkIndexing(df):
    holder, ogDF = list(df.iloc[:,0]), df
    time2 = holder[1:]
    lst = [t2 - t1 for t1, t2 in zip(holder, time2)]
        
    delta = pd.DataFrame(df.iloc[1:,:])
    delta.iloc[:, 0] = lst
    lst = [int(str(i)[:2]) for i in lst]
    d = int(str(delta['DATE'].std())[0])
    irregular = []
    delta.index = [i for i in range(len(delta))]
    
    for i, j in enumerate(lst):
        if not j - d <= 0 <= j + d:
            irregular.append(i)    
            
    for i, j in zip(enumerate(df.iloc[:,0]), irregular):
        if i[0] == j:
            df = df.drop(labels = j)    
            
    df.index = [i for i in range(len(df))]
    
    if delta['DATE'].std() / delta['DATE'].mean() > 1:
        return df
    else:
        return ogDF

rt, gld = checkIndexing(rt), checkIndexing(gld)
tmkt, sch = checkIndexing(tmkt), checkIndexing(sch)

def checkHiddenNulls(df, colIndex):    
    '''check whether columns have any non-null meaningless values'''
    cleaning = df.iloc[:, colIndex]
    for idx, value in zip(range(len(cleaning)), cleaning):
        try:
            if float(value) / float(value) != 1: #filter out n/a values
                df = df.drop(index = idx)
        except:
            df = df.drop(index = idx)
            #same statement as above because above one won't be executed
            #if it raises error
    
    df.iloc[:, colIndex] = df.iloc[:, colIndex].astype(float)
    return df

sch, tmkt = checkHiddenNulls(sch, 1), checkHiddenNulls(tmkt, 1)
reit, gld = checkHiddenNulls(rt, 1), checkHiddenNulls(gld, 1)

def getHTML(url):
    '''extract html via url'''    
    connection = requests.get(url)
    if connection.ok:
        txt = connection.text.lower()
        text = txt[txt.find('<head>'): txt.find('</head>')]
        text = text.split(' ')
    return text

gldText = getHTML('https://fred.stlouisfed.org/series/GOLDPMGBD228NLBM')
reitText = getHTML('https://fred.stlouisfed.org/series/WILLREITIND')
tmktText = getHTML('https://fred.stlouisfed.org/series/WILL5000INDFC')
schillerText = getHTML('https://fred.stlouisfed.org/series/CSUSHPISA')

def seasonallyAdjust(df):
    '''create boolean seasonal indicator variables'''
    df['Spring'], df['Summer'] = False, False
    df['Fall'], df['Winter'] = False, False
    spring, summer = list(df['Spring']), list(df['Summer'])
    fall, winter = list(df['Fall']), list(df['Winter'])
        
    count = 0
    for date in df.iloc[:,0]:
        if date.month == 1 or date.month == 2 or date.month == 3:
            winter[count] = True
            count += 1
        elif date.month == 4 or date.month == 5 or date.month == 6:
            spring[count] = True
            count += 1
        elif date.month == 7 or date.month == 8 or date.month == 9:
            summer[count] = True
            count += 1
        elif date.month == 10 or date.month == 11 or date.month == 12:
            fall[count] = True
            count += 1
            
    df['Fall'], df['Winter'] = fall, winter
    df['Summer'], df['Spring'] = summer, spring
    return df
    
reit, gld = seasonallyAdjust(reit), seasonallyAdjust(gld) 
schiller, tmkt = seasonallyAdjust(tmkt), seasonallyAdjust(sch)

def firstDifference(df, colIndex):
    '''insure against autocorrelation by first-differencing the data'''
    holder = list(df.iloc[:, colIndex])
    time2 = holder[1:]
    lst = [t2 - t1 for t1, t2 in zip(holder, time2)]
    
    delta = pd.DataFrame(df.loc[1:,:]) #was raising error w/o pd.DF wrapper
    delta.iloc[:, colIndex] = lst
    return delta
    
reit, gld = firstDifference(reit, 1), firstDifference(gld, 1)
tmkt, sch = firstDifference(tmkt, 1), firstDifference(schiller, 1)

def unitRootTest(df, colIndex):
    '''Run Dickey-Fuller test on column to check for unit root'''
    x = df.iloc[:, colIndex]
    dfuller = adfuller(x)
    results = {'ADF Statistic': dfuller[0],
            'p-value': dfuller[1], 'Critical Values': dfuller[4]}
    if float(dfuller[1]) < .05:
        results['Reject Null'] = True #no unit root
    else:
        results['Reject Null'] = False #yes unit root
    return results

reitRoot, gldRoot = unitRootTest(reit, 1), unitRootTest(gld, 1)
sRoot, tmktRoot = unitRootTest(schiller, 1), unitRootTest(tmkt, 1)
#if unit root present, too much risk to create linear model

def kurtosisMaxObservation(df, colIndex):
    '''return the percentage of kurtosis due to one observation'''
    data = df.iloc[:, colIndex]
    mean = data.mean()
    num, den = [], []
    
    for i, j in enumerate(data):
        distance = j - mean
        num.append(distance ** 4)
        den.append(distance ** 2)
    for a, b in enumerate(num):
        if b == max(num):
            location = a
    num, den = np.array(num), np.array(den)
    numAv, denAv = num.mean(), (den.mean()) ** 2
    kurtosis = (numAv / denAv) - 3
    if kurtosis == scipy.stats.kurtosis(df.iloc[:, colIndex]):
        proportion = num.max() / sum(num)
        return proportion * 100, location
    else:
        raise Exception('The kurtosis is incorrect')
        
sA, rA = kurtosisMaxObservation(sch, 1), kurtosisMaxObservation(reit, 1)
gA, tA = kurtosisMaxObservation(gld, 1), kurtosisMaxObservation(tmkt, 1)   

def plotKurtosisObs(df, iloc, text, idx, kurtosis, condition):
    '''visualize vectors with max single-obs. kurtosis'''
    text = str(text) #extract title from html
    t = text[text.find('<title>'):text.find('</title>')].strip("")
    t, var = t.split(','), str()
    for i in t:
        var += i
    t = var[7:]
    figure = plt.figure()
    ax = figure.add_subplot(111)
    ax.plot(df.iloc[:, iloc], 'steelblue', 3)
    ax.annotate('Kurtosis: ' + str(round(kurtosis, 3)) + '%',
                xy = (idx, df.iloc[idx,iloc]))
    plt.ylabel('Delta')
    plt.xlabel('Time 0: ' + str(df.iloc[0, 0])[:-9] + '\n'
               'Time n: ' + str(df.iloc[-1, 0])[:-9] + '\n'
               'Unit Root: ' + str(not condition))
    plt.ylim(top = 1.5 * max(df.iloc[:,iloc]))
    plt.title(t)
    plt.show()

plotKurtosisObs(sch, 1, schillerText, sA[1], sA[0], sRoot['Reject Null'])
plotKurtosisObs(reit, 1, reitText, rA[1], rA[0], reitRoot['Reject Null'])
plotKurtosisObs(gld, 1, gldText, gA[1], gA[0], gldRoot['Reject Null'])
plotKurtosisObs(tmkt, 1, tmktText, tA[1], tA[0], tmktRoot['Reject Null'])
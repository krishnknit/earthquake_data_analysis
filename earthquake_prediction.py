"""
Created on Wed Feb 9 09:17:47 2017
@author: ankurD
"""
import json
import urllib.request
import time
import config
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor
from logger import Logger

log = Logger().get_logger()

sns.set(color_codes=True)
URL = config.EARTHQUAKE_URL

def check_attribute(data, **kwargs):
    """This module checks for geographcal attributes for each line """

    global const_bigdata
    dictattr = {}
    for feature in data['features']:
        for key in feature:
            if key == "geometry"or key == "properties":
                value = feature[key]
                for attr in value:
                    if key +"_"+attr in dictattr:
                        dictattr[key +"_"+attr].append(value[attr])
                    else:
                        dictattr[key +"_"+attr] = [value[attr]]

    if len(kwargs) == 0:
        const_bigdata = pd.DataFrame(dictattr)
    else:
        const_bigdata = const_bigdata.append(pd.DataFrame(dictattr), ignore_index=True)
    return const_bigdata

def check_date(ytdstatus):
    """This module set startdate and enddate for url """

    firstday = "2020-01-01"
    i = 1
    if ytdstatus is False:
        pagesrc = open_url(URL+"&starttime=" + firstday + "&endtime=2019-2-1")
        data = load_data(pagesrc)
        check_attribute(data)
    else:
        lastday = datetime.now().strftime("%Y-%m-%d")
        newdate1 = time.strptime(firstday, "%Y-%m-%d")
        newdate2 = time.strptime(lastday, "%Y-%m-%d")
        if (newdate1 < newdate2) is True:
            pass
        stoploop = 0

        for year in range(int(firstday[:4]), int(lastday[:4])+1):
            for month in range(1, 13):
                startdt = str(year)+ "-" + ("0" +str(month))[-2:] + "-01"
                if month == 12:
                    enddt = str(year+1)+ "-01-01"
                    if newdate2 < time.strptime(enddt, "%Y-%m-%d"):
                        stoploop = 1
                else:
                    enddt = str(year)+ "-" + ("0"+str(month+1))[-2:] + "-01"
                    if newdate2 < time.strptime(enddt, "%Y-%m-%d"):
                        enddt = lastday
                        stoploop = 1
                newdate1 = time.strptime(firstday, "%Y-%m-%d")
                newdate2 = time.strptime(lastday, "%Y-%m-%d")
                pagesrc = open_url(URL+"&starttime=" + startdt + "&endtime=" + enddt)
                data = load_data(pagesrc)
                if i == 1:
                    const_data = check_attribute(data)
                else:
                    const_data = check_attribute(data, const_data=const_data)
                i = i+1
                if stoploop == 1:
                    create_stats(const_data)
                    break

def load_data(pagesrc):
    """This module loads data in jsn format and store it in data object """
    data = json.loads(pagesrc.decode('utf-8'))
    return data

def open_url(url):
    """This module reads the data from url passed by check_date funcion """
    log.info("Working on..." + url)
    log.info(".....................................................")
    pagesrc = urllib.request.urlopen(url).read()
    return pagesrc

def create_stats(dataframe):
    """This module is used to call the methods to create a statistical graph from the data"""
    create_hist(dataframe)
    create_violin(dataframe)
    create_linechart(dataframe)
    create_stackedchart(dataframe)
    create_piechart(dataframe)

def create_hist(dataframe):
    """This module is used to create a statistical Histogram graph from the data"""
    fig = plt.figure()
    #Create one or more subplots using add_subplot, because you can't create blank figure
    axes = fig.add_subplot(1, 1, 1)
    axes.hist(dataframe['properties_mag'].dropna(), bins=30)
    plt.title('properties_net')
    plt.xlabel('properties_mag')
    plt.ylabel('properties_rms')
    plt.show()

def create_violin(dataframe):
    """This module is used to create a statistical Violin graph from the data"""
    sns.violinplot(dataframe['properties_mag'], const_bigdata['properties_rms']) #Variable Plot
    sns.despine()

def create_linechart(dataframe):
    """This module is used to create a statistical Line Chart from the data"""
    var = dataframe.groupby('properties_sources').properties_sig.sum()
    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.set_xlabel('properties_sources')
    ax1.set_ylabel('Sum of properties_sig')
    ax1.set_title("properties_sources wise Sum of properties_sig")
    var.plot(kind='line')

def create_stackedchart(dataframe):
    """This module is used to create a statistical Stacked Column Chart from the data"""
    var = dataframe.groupby(['properties_net', 'properties_magType']).properties_mag.sum()
    var.unstack().plot(kind='bar', stacked=True, color=['red', 'blue'], grid=False)

def create_piechart(dataframe):
    """This module is used to create a statistical Pie Chart from the data"""

    var        = dataframe.groupby(['properties_net']).sum().stack()
    temp       = var.unstack()
    x_list     = temp['properties_mag']
    label_list = temp.index
    plt.axis("equal") #The pie chart is oval by default.
    #To show the percentage of each pie slice, pass an output format to the autopctparameter
    plt.pie(x_list, labels=label_list, autopct="%1.1f%%")
    plt.title("Pie Chart")
    plt.show()

if __name__ == "__main__":
    log.info("Starting data retrieval...")
    ytds = [False, True]
    for ytd in ytds:
        check_date(ytd)

    log.info("Completed data retrieval...")
    log.info("\nCompleted!!!")

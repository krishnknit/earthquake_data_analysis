"""
Created on Sat Apr 18 16:00:00 2020
@author: krishnknit
"""
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from logger import Logger

log = Logger().get_logger()

sns.set(color_codes=True)


class EQPrediction():
    def __init__(self, config):
        self.base_url        = config.EARTHQUAKE_BASE_URL
        self.threadpool_size = config.THREADPOOL_SIZE
        self.timeout         = config.TIMEOUT
        self.start_date      = config.START_DATE
        self.end_date        = config.END_DATE
        self.const_bigdata = None

    def load_data(self, st, dt):
        """
        Load data for every 10 days in each call
        """
        url  = f"{self.base_url}&starttime={st}&endtime={dt}"
        log.info(f"processing url '{url}'")
        resp = requests.get(url)
        data = resp.json()
        self.check_attribute(data)

    def check_attribute(self, data):
        """
        To check for geographcal attributes for each line
        """
        dictattr = {}
        features = data.get('features')
        for feature in features:
            for prop in feature:
                if prop == "geometry" or prop == "properties":
                    value = feature[prop]
                    for attr in value:
                        if prop + "_" + attr in dictattr:
                            dictattr[prop + "_" + attr].append(value[attr])
                        else:
                            dictattr[prop + "_" + attr] = [value[attr]]

        df = pd.DataFrame(dictattr)

        if not df.empty and self.const_bigdata is None:
            self.const_bigdata = df
        elif not df.empty:
            self.const_bigdata.append(df)
        log.info("Done")

    def create_stats(self):
        """This module is used to call the methods to create a statistical graph from the data"""
        log.info("Histogram shows the analytics as per the magnitude of earthquake\n\n")
        self.create_hist('Normal Distribution Histogram as per the mag', 'properties_mag', 'frequency')
        log.info("Histogram shows the analytics as per the sig of earthquake\n\n")
        self.create_hist('Normal Distribution Histogram as per the sig', 'properties_sig', 'frequency')
        log.info("Histogram shows the analytics as per the dmin of earthquake\n\n")
        self.create_hist('Normal Distribution Histogram as per the dmin', 'properties_dmin', 'frequency')
        log.info("Histogram shows the analytics as per the NST of earthquake\n\n")
        self.create_hist('Normal Distribution Histogram as per the NST', 'properties_nst', 'frequency')
        self.create_violin()
        self.create_linechart()
        self.create_stackedchart()
        self.create_piechart()

    def create_hist(self, heading, x_label, y_label):
        """This module is used to create a statistical Histogram graph from the data"""
        dataframe = self.const_bigdata
        fig = plt.figure()
        #Create one or more subplots using add_subplot, because you can't create blank figure
        axes = fig.add_subplot(1, 1, 1)
        axes.hist(dataframe[x_label].dropna(), bins=30)
        plt.title(heading)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.show()

    def create_violin(self):
        """This module is used to create a statistical Violin graph from the data"""
        dataframe = self.const_bigdata
        sns.violinplot(dataframe['properties_mag'], dataframe['properties_rms'])  # Variable Plot
        sns.despine()

    def create_linechart(self):
        """This module is used to create a statistical Line Chart from the data"""
        dataframe = self.const_bigdata
        var       = dataframe.groupby('properties_sources').properties_sig.sum()
        fig       = plt.figure()
        ax1       = fig.add_subplot(1, 1, 1)
        ax1.set_xlabel('properties_sources')
        ax1.set_ylabel('Sum of properties_sig')
        ax1.set_title("properties_sources wise Sum of properties_sig")
        var.plot(kind='line')

    def create_stackedchart(self):
        """This module is used to create a statistical Stacked Column Chart from the data"""
        dataframe = self.const_bigdata
        var = dataframe.groupby(['properties_net', 'properties_magType']).properties_mag.sum()
        var.unstack().plot(kind='bar', stacked=True, color=['red', 'blue'], grid=False)

    def create_piechart(self):
        """This module is used to create a statistical Pie Chart from the data"""
        dataframe = self.const_bigdata
        var = dataframe.groupby(['properties_net']).sum().stack()
        temp = var.unstack()
        type(temp)
        x_list = temp['properties_mag']
        label_list = temp.index
        plt.axis("equal") #The pie chart is oval by default.
        #To show the percentage of each pie slice, pass an output format to the autopctparameter
        plt.pie(x_list, labels=label_list, autopct="%1.1f%%")
        plt.title("Pie Chart")
        plt.show()

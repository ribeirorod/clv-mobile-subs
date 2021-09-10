# check models final weighted curves

#from numpy.core.defchararray import index
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
#from pandas.core.indexes.base import Index

parent = os.path.dirname(os.getcwd())

periods = {'day': 365 + 1,
            'week': 52 + 1,
            'month': 12 + 1}

def plot_cruves(df, periodicity):
    bp= np.arange(1,periods[periodicity])
    for country in df['country'].unique():
        plt.style.use('seaborn')
        plt.title("%s - weighted curves by carrier" %country)
        plt.ylabel("Attr. rates")
        #plt.xlabel("Attr. rates")
        for carrier in df.loc[df['country']==country, 'network_operator'].unique():
            data = df.loc[(df['country']==country) & (df['network_operator']==carrier)]
            max_week= data['joined_%s'%periodicity].max()
            rates= data.loc[data['joined_%s'%periodicity]==max_week, 'rates'].iloc[:52]
            #print(data.head())
            plt.plot(bp, rates, label=carrier)
        plt.xticks(range(1,periods[periodicity],periods[periodicity]%10))
        plt.legend()
        plt.ylim (-0.1,1.5)
        plt.show()

def get_df(periodicity):

    df = pd.read_csv('/data/final_curves_%s.csv'%periodicity, sep=';')
    df['rate01final'] = 1
    df.drop(labels=df.loc[df['rate02final']==0].index, axis=0, inplace=True)
    df.drop(columns=['pid', 'advertiser_id', 'lifetime'], inplace=True)

    cols= ['joined_%s'%periodicity, 'region', 'country', 'network_operator' ,'advertiser','periodicity']

    df.drop_duplicates(subset=cols, inplace=True)
    df= df.melt(id_vars= cols, var_name='period',value_name='rates').sort_values(by=cols +['period'])
    df['lifetime'] = df.groupby(cols )['rates'].cumsum()
    df.to_csv('/data/weighted_curves_%s.csv'%periodicity, sep=';', index=False)
    #plot_cruves(df, periodicity)
    return df, periodicity

for period in ['week', 'month']:
    get_df(period)

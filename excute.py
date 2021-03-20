import pandas as pd
from pak import Web, TransformDf, detail
import os
import requests


def load():                 # Load data set
    web = Web('url', 'results-table', id_type='id')
    page = 4

    if 'attack.xlsx' in list(os.listdir('data_set')):
        atk_data = pd.read_excel('data_set\\attack.xlsx', index_col=[0])
    else:
        count = 0
        for p in range(1, page+1):
            web.url = "https://www.start.umd.edu/gtd/search/Results.aspx?page={}&casualties_type=b&" \
                  "casualties_max=&start_yearonly=2000&end_yearonly=2019&dtp2=all&country=4&" \
                  "expanded=no&charttype=line&chart=overtime&ob=GTDID&od=desc#results-table".format(str(p))
            temp = web.get_table()
            if count == 0:
                atk_data = temp
            else:
                atk_data = pd.concat([atk_data, temp], ignore_index=True)
            count += 1
        atk_data.to_excel('data_set\\attack.xlsx')
    return atk_data


def clean():
    df_transformer = TransformDf(load())    # Transform data (cleaning)
    df = df_transformer.pre_process()       # pre-processing
    rebal = df_transformer.unpack_col('PERPETRATOR GROUP', ',')
    target = df_transformer.unpack_col('TARGET TYPE', ',')
    return df, rebal.set_index('id'), target.set_index('id')

def get_data():
    df, rebel_group, target_group = clean()
    p_id = 'secondary-left'
    detail_dic = {'GTD ID': [],
                    'Province': []}
    for ndx in df.index:
        dic = detail('https://www.start.umd.edu/gtd/search/IncidentSummary.aspx?gtdid={}'.format(ndx), p_id)
        detail_dic['GTD ID'].append(dic['GTD ID:'])
        detail_dic['Province'].append(dic['Province/administrativeregion/u.s. state:'])

    detail_table = pd.DataFrame(detail_dic).set_index('GTD ID')
    fact_table = pd.merge(df, detail_table, left_index=True, right_index=True)
    return fact_table, rebel_group, target_group


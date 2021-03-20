import requests
from bs4 import BeautifulSoup
import lxml
import pandas as pd


class Web:

    def __init__(self, url, p_id, id_type='class'):
        self.p_id = p_id
        self.url = url
        self.id_type = id_type

    def get_table(self):
        req = requests.get(self.url).text
        soup = BeautifulSoup(req, 'html.parser')
        tbl = soup.find_all('table', {'class' if self.id_type == 'class' else 'id': self.p_id})
        df = pd.read_html(str(tbl))[0]
        return df

    def check_table(self):
        req = requests.get(self.url).text
        soup = BeautifulSoup(req, 'html.parser')
        return len(soup.find_all('table', {'class' if self.id_type == 'class' else 'id': self.p_id})) == 1


class TransformDf:
    def __init__(self, df):
        self.df = df

    def unpack_col(self, column, delimiter):
        df_t = {'id': [],
                'ty': []}
        for ndx in self.df.index:
            for ty in self.df.loc[ndx, column].split(delimiter):
                df_t['id'].append(ndx)
                df_t['ty'].append(ty)
        return pd.DataFrame(df_t)

    def pre_process(self):
        self.df['CITY'] = self.df['CITY'].fillna('Unknown')
        self.df['PERPETRATOR GROUP'] = self.df['PERPETRATOR GROUP'].replace(['Unknown (suspected)'], 'Unknown')
        self.df['FATALITIES'] = self.df['FATALITIES'].replace(['Unknown'], 0)
        self.df['INJURED'] = self.df['INJURED'].replace(['Unknown'], 0)
        self.df = self.df.astype({'GTD ID': 'str',
                                  'COUNTRY': 'str',
                                  'CITY': 'str',
                                  'PERPETRATOR GROUP': 'str',
                                  'FATALITIES': 'int16',
                                  'INJURED': 'int16',
                                  'TARGET TYPE': 'str'})
        for ndx in self.df.index:
            self.df.loc[ndx,'CITY'] = self.df['CITY'][ndx].replace('district', '').strip()

        self.df = self.df.set_index('GTD ID')
        return self.df


def detail(url, p_id):
    dic = {}
    try:
        req = requests.get(url).text
        soup = BeautifulSoup(req, 'html.parser')
        info_area = soup.find(id=p_id)
        for i in range(len(info_area.find_all('span', {'class': 'leftHead'}))):
            dic[info_area.find_all('span', {'class': 'leftHead'})[i].get_text()] = info_area.find_all('span', {'class': 'leftLarge'})[i].get_text()
    except Exception:
        print(url)
        pass
    return dic

# see http://dept.stat.lsa.umich.edu/~kshedden/Python-Workshop/nhanes_data.html

import xport.v56
from glob import glob
from pathlib import Path
import pandas as pd
import requests
import os
from time import sleep
import numpy as np
import string
from bs4 import BeautifulSoup

def load_raw_NHANES_data(datafile_path):
    datafiles = glob(str(datafile_path / '*XPT'))

    alldata = {}
    data_dict = {}

    for datafile in datafiles:
        with open(datafile, 'rb') as infile:
            print('opening', datafile)
            # first need to load using xport to get metadata
            xp = xport.v56.load(infile)
            xp_key = list(xp.keys())[0]
            metadata_df = xp[xp_key].contents
            for index in metadata_df.index:
                data_dict[metadata_df.loc[index, 'Variable']] = metadata_df.loc[index, 'Label']
            # then load data itself with pandas
            df = pd.read_sas(datafile)
            alldata[xp_key] = df.set_index('SEQN')

    return(alldata, data_dict)


def join_all_dataframes(alldata):
    nhanes_df = None
    for key in alldata:
        if nhanes_df is None:
            nhanes_df = alldata[key]
        else:
            nhanes_df = nhanes_df.join(alldata[key])
    return(nhanes_df)


def get_nhanes_year_code_dict(latest_year=2018):
    year_codes = {}
    year_letters = string.ascii_uppercase[1:]
    for index, year in enumerate(range(2001, latest_year, 2)):
        year_str = '%d-%d' % (year, year+1)
        year_codes[year_str] = year_letters[index]
    return(year_codes)


def download_raw_datafiles(datadir='../data',
                           year='2017-2018', 
                           baseurl='https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018',
                           datasets=None):

    year_codes = get_nhanes_year_code_dict()
    assert year in year_codes

    dataset_dir = os.path.join(datadir, year)
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir)

    if datasets is None:
        datasets = [
            'HSQ', 'DBQ', 'CBC', 'DLQ', 'HIQ', 'SLQ', 'DPQ', 'SMQRTU',
            'PFQ', 'ACQ', 'BPX', 'DR2TOT', 'BMX', 'HDL', 'TCHOL',
            'PAQ', 'MCQ', 'FASTQX', 'DUQ', 'PBCD', 'DEMO', 'ECQ',
            'DXX', 'DR1TOT', 'DR2TOT', 'CDQ', 'DIQ', 'DEQ', 'GHB',
            'SMQ', 'WHQ', 'BPQ']
    for dataset in datasets:
        dataset_url = '/'.join([baseurl, '%s_%s.XPT' % (dataset, year_codes[year])])
        print('downloading', dataset_url)
        # random delay to prevent web server from getting upset with us
        sleep(np.random.rand())
        r = requests.get(dataset_url, allow_redirects=True)
        datafile_name = os.path.join(
            dataset_dir,
            os.path.basename(dataset_url))

        with open(datafile_name, 'wb') as f:
            f.write(r.content)
        
        # grab html documentation for each dataset
        doc_dir = os.path.join(
            '%s_docs' % datadir,
            year)
        if not os.path.exists(doc_dir):
            os.makedirs(doc_dir)

        doc_url = dataset_url.replace('XPT', 'htm')
        print('downloading', doc_url)
        sleep(np.random.rand())
        r = requests.get(doc_url, allow_redirects=True)
        docfile_name = os.path.join(
            doc_dir,
            os.path.basename(doc_url))
        with open(docfile_name, 'wb') as f:
            f.write(r.content)


def parse_html_variable_info_section(info):
    infodict = {
        i[0].text.strip(': ').replace(' ', ''): i[1].text
        for i in zip(info.find_all('dt'), info.find_all('dd'))
    }
    return(infodict)

def parse_nhanes_html_docs(docfile):
    variable_df = pd.DataFrame()

    with open(docfile, 'r') as f:
        soup = BeautifulSoup('\n'.join(f.readlines()), 'html.parser')
    variable_titles = soup.find_all('h3', {'class':'vartitle'}) 
    variable_info = soup.find_all('dl')
    for title, info in zip(variable_titles, variable_info):
        print(title, info)
        infodict = parse_html_variable_info_section(info)
        assert title.get('id') == infodict['VariableName']
        for key in infodict:
            if key != 'VariableName':
                variable_df.loc[title.get('id'), key] = infodict[key]

if __name__ == "__main__":
    datafile_path = Path('../data/2017-2018')
    alldata, data_dict = load_raw_NHANES_data(datafile_path)

    nhanes_df = join_all_dataframes(alldata)



soup_table = soup.find("table")
soup_table_data = soup_table.tbody.find_all("tr")  # contains 2 rows

# Get all the headings of Lists
headings = []
for td in soup_table_data[0].find_all("td"):
    # remove any newlines and extra spaces from left and right
    headings.append(td.text.replace('\n', ' ').strip())

print(headings)

docdata = {}
for table, heading in zip(soup.find_all("table"), headings):
    # Get headers of table i.e., Rank, Country, GDP.
    t_headers = []
    for th in table.find_all("th"):
        # remove any newlines and extra spaces from left and right
        t_headers.append(th.text.replace('\n', ' ').strip())
    # Get all the rows of table
    table_data = []
    for tr in table.tbody.find_all("tr"): # find all tr's from table's tbody
        t_row = {}
        # Each table row is stored in the form of
        # t_row = {'Rank': '', 'Country/Territory': '', 'GDP(US$million)': ''}

        # find all td's(3) in tr and zip it with t_header
        for td, th in zip(tr.find_all("td"), t_headers): 
            t_row[th] = td.text.replace('\n', '').strip()
        table_data.append(t_row)

    # Put the data for the table with his heading.
    data[heading] = table_data

print(data)
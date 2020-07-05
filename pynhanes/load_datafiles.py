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
from collections import Counter

from utils import get_nhanes_year_code_dict, get_source_code_from_filepath, EmptySectionError


def download_raw_datafiles(datadir='../raw_data',
                           docdir='../data_docs',
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
            'PFQ', 'ACQ', 'BPX', 'BMX', 'HDL', 'TCHOL',
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
        doc_dir_year = os.path.join(
            docdir,
            year)
        if not os.path.exists(doc_dir_year):
            os.makedirs(doc_dir_year)

        doc_url = dataset_url.replace('XPT', 'htm')
        print('downloading', doc_url)
        sleep(np.random.rand())
        r = requests.get(doc_url, allow_redirects=True)
        docfile_name = os.path.join(
            doc_dir_year,
            os.path.basename(doc_url))
        with open(docfile_name, 'wb') as f:
            f.write(r.content)


def load_raw_NHANES_data(datafile_path):
    datafiles = glob(str(datafile_path / '*XPT'))
    if len(datafiles) == 0:
        print('no data files available - downloading')
        # assume year is encoded in datafile_path
        year = datafile_path.name
        assert year in get_nhanes_year_code_dict()
        download_raw_datafiles(year=year)
        datafiles = glob(str(datafile_path / '*XPT'))
        if len(datafiles) == 0:
            raise Exception('no data files available and unable to download')

    alldata = {}
    metadata = None

    for datafile in datafiles:
        source_code, metadata_df = get_metadata_from_xpt(datafile)
        metadata_df['Source'] = source_code.split('_')[0]
        metadata_df = metadata_df.query('Variable != "SEQN"')
        metadata_df.index = metadata_df['Variable'] + '_' + metadata_df['Source']
        del metadata_df['Length']
        del metadata_df['Position']
        
        # then load data itself with pandas
        df = pd.read_sas(datafile)

        alldata[source_code] = df.set_index('SEQN')
        if metadata is None:
            metadata = metadata_df
        else:
            metadata = pd.concat((metadata, metadata_df))

    return(alldata, metadata)


def get_metadata_from_xpt(datafile):
    # need to load using xport to get metadata
    with open(datafile, 'rb') as infile:
        xp = xport.v56.load(infile)
    xp_key = list(xp.keys())[0]
    return(xp_key, xp[xp_key].contents)


def load_nhanes_documentation(doc_path):
    docfiles = glob(str(doc_path / '*htm'))
    variable_dfs = {}
    variable_code_tables = {}

    for docfile in docfiles:
        print('parsing docfile',docfile)
        doc_code = get_source_code_from_filepath(docfile)
        variable_dfs[doc_code], code_tables = parse_nhanes_html_docfile(docfile)
        variable_code_tables.update(code_tables)

    variable_df = None
    for code in variable_dfs:
        if variable_df is None:
            variable_df = variable_dfs[code]
        else:
            variable_df = pd.concat((variable_df, variable_dfs[code]))
    return(variable_df, variable_code_tables)


def join_all_dataframes(alldata):
    nhanes_df = None
    for key in alldata:
        nhanes_df = alldata[key] if nhanes_df is None else nhanes_df.join(
            alldata[key], rsuffix=key)
    return(nhanes_df)


def parse_nhanes_html_docfile(docfile):
    variable_df = pd.DataFrame()
    variable_code_tables = {}
    source_code = get_source_code_from_filepath(docfile)

    with open(docfile, 'r') as f:
        soup = BeautifulSoup('\n'.join(f.readlines()), 'html.parser')

    # each variable is described in a separate div
    for section in soup.find_all('div'):
        try:
            variable_df, variable_code_tables = parse_html_variable_section(
                section, variable_df, variable_code_tables, docfile)
        except EmptySectionError:
            pass
    
    variable_df = variable_df.loc[variable_df.index != 'SEQN_%s' % source_code, :] 
    # check for duplicated long variable names
    variable_df, variable_code_tables = deduplicate_long_variable_names_within_set(
        variable_df, variable_code_tables)

    return((variable_df, variable_code_tables))


def parse_html_variable_section(section, variable_df, variable_code_tables, docfile):
    title = section.find('h3', {'class': 'vartitle'})
    source_code = get_source_code_from_filepath(docfile)

    if title is None:
        raise EmptySectionError

    info = section.find('dl')

    infodict = parse_html_variable_info_section(info)
    assert title.get('id') == infodict['VariableName']

    index_variable = 'VariableName'
    infodict['index'] = '%s_%s' % (infodict[index_variable], source_code)        
 
    for key in infodict:
        if key != 'index':
            # need to fix overlapping names for dietary recall
            if source_code in ['DR1TOT', 'DR2TOT']:
                infodict[key] = '%s_%s' % (infodict[key], source_code.replace('TOT', ''))
            variable_df.loc[infodict[index_variable], key] = infodict[key]

    table = section.find('table')
    if table is not None:
        variable_code_tables[infodict['index']] = pd.read_html(str(table))[0]

    variable_df['Source'] = source_code
    return((variable_df, variable_code_tables))


# for deduplicating within a single variable set
def deduplicate_long_variable_names_within_set(variable_df):
    variable_df = variable_df.query('VariableNameLong != "RespondentSequenceNumber"')
    variable_counts = variable_df.VariableNameLong.value_counts()
    repeated_variables = variable_counts[variable_counts > 1]
    repeated_df = variable_df[variable_df.VariableNameLong.isin(repeated_variables.index)]  
    for ctr, index in enumerate(repeated_df.index):
        variable_df.loc[index, 'VariableNameLong'] = '%s_%d' % (
            variable_df.loc[index, 'VariableNameLong'], ctr + 1)

    return(variable_df)


# for deduplicating once everything is combined
def deduplicate_long_variable_names_across_sets(variable_df):
    variable_df = variable_df.query('VariableNameLong != "RespondentSequenceNumber"')
    variable_counts = variable_df.VariableNameLong.value_counts()
    repeated_variables = variable_counts[variable_counts > 1]
    repeated_df = variable_df[variable_df.VariableNameLong.isin(repeated_variables.index)]  
    for ctr, index in enumerate(repeated_df.index):
        variable_df.loc[index, 'VariableNameLong'] = '%s_%s' % (
            variable_df.loc[index, 'VariableNameLong'],
            variable_df.loc[index, 'Source'])

    return(variable_df)

def parse_html_variable_info_section(info):
    infodict = {
        i[0].text.strip(': ').replace(' ', ''): i[1].text
        for i in zip(info.find_all('dt'), info.find_all('dd'))
    }

    infodict['VariableNameLong'] = ''.join([i.title() for i in infodict['SASLabel'].translate(str.maketrans('', '', string.punctuation)).split(' ')]) if 'SASLabel' in infodict else infodict['VariableName']

    return(infodict)


def rename_nhanes_vars(nhanes_df, variable_df):
    rename_dict = {}
    for i in variable_df.index:
        rename_dict[i] = variable_df.loc[i, 'VariableNameLong']
    return(nhanes_df.rename(columns=rename_dict))


def recode_nhanes_vars(nhanes_df, variable_df, variable_code_tables):
    for variable in nhanes_df.columns:
        variable_info = variable_df.loc[variable,]
        if variable not in variable_code_tables:
            print('missing', variable)
    

def save_combined_data(nhanes_df, output_path='../combined_data'):
    combined_data_path = Path(output_path)
    if not combined_data_path.exists():
        combined_data_path.mkdir()

    nhanes_df.to_csv(combined_data_path / str('NHANES_data_%s.tsv' % year), sep='\t')


if __name__ == "__main__":
    year = '2017-2018'
    datafile_path = Path('../raw_data/%s' % year)
    alldata, metadata = load_raw_NHANES_data(datafile_path)

    doc_path = Path('../data_docs/%s' % year)
    variable_df, variable_code_tables = load_nhanes_documentation(doc_path)

    variable_df = deduplicate_long_variable_names_across_sets(variable_df)

    nhanes_df = join_all_dataframes(alldata)

    nhanes_df_recoded = recode_nhanes_vars(nhanes_df, variable_code_tables)

    nhanes_df_renamed = rename_nhanes_vars(nhanes_df_recoded, variable_df)

    save_combined_data(nhanes_df_renamed)

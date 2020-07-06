"""
use data from CDC to create combined NHANES data file
"""


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
import argparse
import pickle

from utils import get_nhanes_year_code_dict, get_source_code_from_filepath
from utils import EmptySectionError, make_long_variable_name
from utils import get_vars_to_keep


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
            'HSQ', 'DBQ', 'DLQ', 'HIQ', 'SLQ', 'DPQ', 'SMQRTU',
            'PFQ', 'BPX', 'BMX', 'HDL', 'TCHOL',
            'PAQ', 'MCQ', 'DUQ', 'PBCD', 'DEMO',
            'DXX', 'DR1TOT', 'DR2TOT', 'DIQ', 'GHB',
            'SMQ', 'WHQ']
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


def load_raw_NHANES_data(datafile_path,
                         vars_to_keep_file='vars_to_keep.json'):
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
        dataset_code = source_code.split('_')[0]  # remove year code
        metadata_df['Source'] = dataset_code
        metadata_df = metadata_df.query('Variable != "SEQN"')
        metadata_df.index = metadata_df['Variable'] + '_' + metadata_df['Source']
        del metadata_df['Length']
        del metadata_df['Position']
        metadata_df = add_long_variable_names_to_metadata(metadata_df)
        metadata_df = deduplicate_long_variable_names_within_set(metadata_df)
        if metadata is None:
            metadata = metadata_df
        else:
            metadata = pd.concat((metadata, metadata_df))

        # then load data itself with pandas
        df = pd.read_sas(datafile).set_index('SEQN')
        if dataset_code in get_vars_to_keep(vars_to_keep_file):
            df = df[get_vars_to_keep(vars_to_keep_file)[dataset_code]]
        # add source code to column name
        df.columns = ['%s_%s' % (i, dataset_code) for i in df.columns]

        alldata[source_code] = df

    metadata = deduplicate_long_variable_names_across_sets(metadata)
    return(alldata, metadata)


def add_long_variable_names_to_metadata(metadata):
    for i in metadata.index:
        metadata.loc[i, 'VariableNameLong'] = make_long_variable_name(metadata.loc[i, 'Label'])
    return(metadata)


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
        print('parsing docfile', docfile)
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
    variable_df.index = variable_df.VariableName + '_' + variable_df.Source
    return((variable_df, variable_code_tables))


def parse_html_variable_section(section, variable_df, variable_code_tables, docfile):
    title = section.find('h3', {'class': 'vartitle'})
    source_code = get_source_code_from_filepath(docfile)

    if title is None or title.text.find('CHECK ITEM') > -1:
        raise EmptySectionError

    info = section.find('dl')

    infodict = parse_html_variable_info_section(info)
    assert title.get('id') == infodict['VariableName']

    infodict['VariableName'] = infodict['VariableName'].upper()
    index_variable = 'VariableName'
    infodict['index'] = '%s_%s' % (infodict[index_variable], source_code)

    for key in infodict:
        if key != 'index':
            variable_df.loc[infodict[index_variable], key] = infodict[key]

    table = section.find('table')
    if table is not None:
        infotable = pd.read_html(str(table))[0]
        variable_code_tables[infodict['index']] = infotable

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


def recode_to_float_if_possible(value_to_recode):
    try:
        return(float(value_to_recode))
    except ValueError:
        return(value_to_recode)


def recode_nhanes_vars(nhanes_df, metadata, variable_code_tables,
                       refused_as_na=True, dontknow_as_na=True,
                       table_length_thresh=20):
    nhanes_df_recoded = nhanes_df.copy()
    metadata['Recoded'] = False
    for variable in nhanes_df.columns:
        assert variable in metadata.index
        assert variable in variable_code_tables
        table = variable_code_tables[variable]
        table = table.loc[~table['Value Description'].str.match('Missing')]

        if table.shape[0] == 1 or table['Value Description'].str.match('Range of Values').any():
            continue
        if table['Value Description'].str.match('Value was recorded').any():
            continue
        # kludge for certain variables that have many different values
        if table.shape[0] > table_length_thresh:
            continue

        recode_dict = {}

        if refused_as_na:  # and nhanes_df[variable].dtype != 'float64':
            refused_idx = table['Value Description'] == 'Refused'
            if refused_idx.sum() > 0:
                refused_val = table.loc[refused_idx, 'Code or Value'].iloc[0]
                refused_val = recode_to_float_if_possible(refused_val)
                recode_dict[refused_val] = np.nan
                table = table.loc[table['Value Description'] != 'Refused']

        if dontknow_as_na:  # and nhanes_df[variable].dtype != 'float64':
            dontknow_idx = table['Value Description'] == "Don't know"
            if dontknow_idx.sum() > 0:
                dontknow_val = table.loc[dontknow_idx, 'Code or Value'].iloc[0]
                dontknow_val = recode_to_float_if_possible(dontknow_val)
                recode_dict[dontknow_val] = np.nan
                table = table.loc[table['Value Description'] != "Don't know"]

        for table_index in table.index:
            recoded_value = table.loc[table_index, 'Value Description']
            value_to_recode = table.loc[table_index, 'Code or Value']
            try:
                value_to_recode = float(value_to_recode)
            except ValueError:
                pass
            recode_dict[value_to_recode] = recoded_value
        metadata.loc[variable, 'Recoded'] = True
        nhanes_df_recoded[variable] = nhanes_df[variable].replace(to_replace=recode_dict)
    return((nhanes_df_recoded, metadata))


def rename_nhanes_vars(nhanes_df, metadata_df):
    rename_dict = {}
    for i in metadata_df.index:
        rename_dict[i] = metadata_df.loc[i, 'VariableNameLong']
    return(nhanes_df.rename(columns=rename_dict))


def save_combined_data(nhanes_df, metadata, variable_code_tables, year,
                       output_path='../combined_data'):
    combined_data_path = Path(output_path) / year
    if not combined_data_path.exists():
        combined_data_path.mkdir()

    nhanes_df.to_csv(combined_data_path / str('NHANES_data_%s.tsv' % year), sep='\t')
    metadata.to_csv(combined_data_path / str('NHANES_metadata_%s.tsv' % year), sep='\t')
    with open(combined_data_path / str('NHANES_variable_coding_%s.pkl' % year), 'wb') as f:
        pickle.dump(variable_code_tables, f)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Load and save NHANES data')
    parser.add_argument('-y', '--year', default='2017-2018',
                        help='year range of dataset collection')
    args = parser.parse_args()
    year = args.year

    datafile_path = Path('../raw_data/%s' % year)
    alldata, metadata = load_raw_NHANES_data(datafile_path)

    doc_path = Path('../data_docs/%s' % year)
    variable_df, variable_code_tables = load_nhanes_documentation(doc_path)

    metadata = metadata.join(variable_df, rsuffix='_variable_df')
    nhanes_df = join_all_dataframes(alldata)

    nhanes_df_recoded, metadata = recode_nhanes_vars(nhanes_df, metadata, variable_code_tables)

    nhanes_df_renamed = rename_nhanes_vars(nhanes_df_recoded, metadata)

    save_combined_data(nhanes_df_renamed, metadata, variable_code_tables, year)
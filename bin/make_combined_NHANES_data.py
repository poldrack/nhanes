#!/usr/bin/env python3
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
import pkg_resources

from nhanes.utils import get_nhanes_year_code_dict, get_source_code_from_filepath
from nhanes.utils import EmptySectionError, make_long_variable_name
from nhanes.utils import get_vars_to_keep, get_datasets


def download_raw_datafiles(datasets=None,
                           datasets_file=None,
                           basedir='./',
                           year='2017-2018',
                           baseurl='https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018'):

    year_codes = get_nhanes_year_code_dict()
    assert year in year_codes
    datadir = os.path.join(basedir, 'raw_data')
    docdir = os.path.join(basedir, 'data_docs')

    dataset_dir = os.path.join(datadir, year)
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir)

    if datasets is None:
        if datasets_file is None:
            datasets_file = pkg_resources.resource_filename(
                'nhanes', 'config/datasets.json')
        datasets = get_datasets(datasets_file)

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


def load_raw_NHANES_data(basedir='./',
                         year='2017-2018',
                         vars_to_keep_file=None,
                         datasets_file=None):
    assert year in get_nhanes_year_code_dict()
    if vars_to_keep_file is None:
        vars_to_keep_file = pkg_resources.resource_filename(
            'nhanes', 'config/vars_to_keep.json')

    if datasets_file is None:
        datasets_file = pkg_resources.resource_filename(
            'nhanes', 'config/datasets.json')
    datasets = get_datasets(datasets_file)

    datafile_path = Path(basedir) / 'raw_data' / year
    datafiles = glob(str(datafile_path / '*XPT'))
    datasets_to_download = []

    for dataset in datasets:
        matching_dataset = [i for i in datafiles if i.find(dataset) > -1]
        if not matching_dataset:
            datasets_to_download.append(dataset)

    if datasets_to_download:
        print('downloading missing data files')
        download_raw_datafiles(
            datasets=datasets_to_download,
            basedir=basedir,
            year=year)
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


def load_nhanes_documentation(basedir='./', year='2017-2018'):
    doc_path = Path(basedir) / 'data_docs' / year
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
    recode_dict = {}
    for variable in nhanes_df.columns:
        variable_shortname = '%s_%s' % (metadata.loc[
            variable].Variable, metadata.loc[variable].Source)
        assert variable in metadata.index
        assert variable_shortname in variable_code_tables
        table = variable_code_tables[variable_shortname]
        table = table.loc[~table['Value Description'].str.match('Missing')]

        if table.shape[0] == 1 or table['Value Description'].str.match('Range of Values').any():
            continue
        if table['Value Description'].str.match('Value was recorded').any():
            continue
        # kludge for certain variables that have many different values
        if table.shape[0] > table_length_thresh:
            continue

        recode_dict[variable] = {}

        # some variables include a very small value in place of zero
        small_values_df = nhanes_df_recoded.query('%s < 1e-6' % variable)
        if small_values_df.shape[0] > 0:
            print('recoding zero for', variable)
            nhanes_df_recoded.loc[small_values_df.index, variable] = 0
            metadata.loc[variable, 'CustomRecoding'] = 'FloatZero'

        if refused_as_na:  # and nhanes_df[variable].dtype != 'float64':
            recode_dict[variable], table = replace_val_in_table(
                'Refused', recode_dict[variable], table)

        if dontknow_as_na:  # and nhanes_df[variable].dtype != 'float64':
            recode_dict[variable], table = replace_val_in_table(
                "Don't know", recode_dict[variable], table)

        for table_index in table.index:
            recoded_value = table.loc[table_index, 'Value Description'].replace(',', '')
            value_to_recode = table.loc[table_index, 'Code or Value']
            try:
                value_to_recode = float(value_to_recode)
            except ValueError:
                pass
            recode_dict[variable][value_to_recode] = recoded_value

        metadata.loc[variable, 'Recoded'] = True
        nhanes_df_recoded[variable] = nhanes_df_recoded[variable].replace(
            to_replace=recode_dict[variable])

    nhanes_df_recoded = apply_custom_recoding(nhanes_df_recoded, metadata)
    return((nhanes_df_recoded, metadata))


def apply_custom_recoding(nhanes_df_recoded,
                          metadata,
                          recode_yesno=True):

    for variable in nhanes_df_recoded.columns:
        if recode_yesno and nhanes_df_recoded[variable].isin(['Yes', 'No']).sum() > 0:
            nhanes_df_recoded[variable] = nhanes_df_recoded[variable].replace(
                to_replace=yesno_recoder())
            metadata.loc[variable, 'CustomRecoding'] = 'YesNo'

        # heuristic to find income variables
        if nhanes_df_recoded[variable].isin(list(income_recoder().keys())).sum() > 0:
            nhanes_df_recoded[variable] = nhanes_df_recoded[variable].replace(
                to_replace=income_recoder())
            metadata.loc[variable, 'CustomRecoding'] = 'Income'

        # depression questionnaire variables
        if nhanes_df_recoded[variable].isin(['More than half the days']).sum() > 0:
            nhanes_df_recoded[variable] = nhanes_df_recoded[variable].replace(
                to_replace=depression_recoder())
            metadata.loc[variable, 'CustomRecoding'] = 'Depression'

        # frequency variables
        if nhanes_df_recoded[variable].isin(['A few times a year']).sum() > 0:
            nhanes_df_recoded[variable] = nhanes_df_recoded[variable].replace(
                to_replace=howoften_recoder())
            metadata.loc[variable, 'CustomRecoding'] = 'HowOften'

    return(nhanes_df_recoded)


def yesno_recoder():
    return({'Yes': 1, 'No': 0})


def howoften_recoder():
    return({
        'Never': 0,
        'A few times a year': 1,
        'Monthly': 2,
        'Weekly': 3,
        'Daily': 4})


def depression_recoder():
    return({
        'Not at all': 0,
        'Several days': 1,
        'More than half the days': 2,
        'Nearly every day': 3})


def income_recoder():
    return({
        '$ 0 to $ 4999': 2000,
        '$ 5000 to $ 9999': 7500,
        '$10000 to $14999': 12500,
        '$15000 to $19999': 17500,
        '$20000 to $24999': 22500,
        '$25000 to $34999': 30000,
        '$35000 to $44999': 40000,
        '$45000 to $54999': 50000,
        '$55000 to $64999': 60000,
        '$65000 to $74999': 70000,
        '$75000 to $99999': 87500,
        '$100000 and Over': 100000,
        'Under $20000': np.nan,
        '$20000 and Over': np.nan})


def replace_val_in_table(value, recode_dict, table, replacement=np.nan):
    replacement_idx = table['Value Description'] == value
    if replacement_idx.sum() > 0:
        replacement_val = table.loc[replacement_idx, 'Code or Value'].iloc[0]
        replacement_val = recode_to_float_if_possible(replacement_val)
        recode_dict[replacement_val] = replacement
        table = table.loc[table['Value Description'] != value]
    return((recode_dict, table))


def remove_extra_variables_from_metadata(data_df, metadata_df):
    return(metadata_df.loc[metadata_df.index.isin(data_df.columns)])


def rename_nhanes_vars(nhanes_df, metadata_df):
    rename_dict = {}
    for i in metadata_df.index:
        rename_dict[i] = metadata_df.loc[i, 'VariableNameLong']
    nhanes_df_renamed = nhanes_df.rename(columns=rename_dict)
    metadata_df = metadata_df.set_index('VariableNameLong')
    return((nhanes_df_renamed, metadata_df))


def get_variable_nonNA_counts(data_df, metadata_df):
    for variable in data_df.columns:
        metadata_df.loc[variable, 'nNonNA'] = data_df[
            variable].notna().sum()
    return(metadata_df)


def save_combined_data(nhanes_df, metadata, variable_code_tables, year,
                       basedir):
    output_path = os.path.join(basedir, 'combined_data')
    combined_data_path = Path(output_path) / year
    if not combined_data_path.exists():
        combined_data_path.mkdir(parents=True)

    nhanes_df.to_csv(combined_data_path / str('NHANES_data_%s.tsv' % year), sep='\t')
    metadata.to_csv(combined_data_path / str('NHANES_metadata_%s.tsv' % year), sep='\t')
    with open(combined_data_path / str('NHANES_variable_coding_%s.pkl' % year), 'wb') as f:
        pickle.dump(variable_code_tables, f)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Load and save combined NHANES data')
    parser.add_argument('-y', '--year', default='2017-2018',
                        help='year range of dataset collection')
    parser.add_argument('-v', '--varfile',
                        help='json file to specify variables to keep')
    parser.add_argument('-d', '--datasetfile',
                        help='json file to specify datasets to include')
    parser.add_argument('-b', '--basedir',
                        help='base directory for data files')

    args = parser.parse_args()
    print(args)
    if args.basedir is None:
        args.basedir = './NHANES'

    alldata, metadata = load_raw_NHANES_data(args.basedir, args.year, args.varfile, args.datasetfile)

    variable_df, variable_code_tables = load_nhanes_documentation(args.basedir, args.year)

    metadata = metadata.join(variable_df, rsuffix='_variable_df')

    nhanes_df = join_all_dataframes(alldata)

    metadata = remove_extra_variables_from_metadata(nhanes_df, metadata)

    nhanes_df_renamed, metadata = rename_nhanes_vars(nhanes_df, metadata)

    nhanes_df_recoded, metadata = recode_nhanes_vars(nhanes_df_renamed, metadata, variable_code_tables)

    metadata = get_variable_nonNA_counts(nhanes_df_recoded, metadata)

    save_combined_data(nhanes_df_recoded, metadata, variable_code_tables, args.year, args.basedir)

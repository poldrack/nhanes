"""
functions to load combined data
"""

import pkg_resources
import pandas as pd
import webbrowser
from .utils import get_nhanes_year_code_dict


def load_NHANES_data(year='2017-2018', datafile=None):
    """
    load NHANES data for a specified year from package

    Parameters:
    -----------
    year: string, denotes year code for data
          (default = '2017-2018')

    Returns:
    ---------
    a pandas data frame containing the data
    """
    if datafile is None:
        datafile = pkg_resources.resource_filename(
            'nhanes', 'combined_data/%s/NHANES_data_%s.tsv' % (year, year))
    return(pd.read_csv(datafile, sep='\t',
                       index_col=0, low_memory=False))


def load_NHANES_metadata(year='2017-2018', datafile=None):
    """
    load NHANES per-variable metadata for a specified year from package

    Parameters:
    -----------
    year: string, denotes year code for data
          (default = '2017-2018')

    Returns:
    ---------
    a pandas data frame containing the metadata
    """
    if datafile is None:
        datafile = pkg_resources.resource_filename(
            'nhanes', 'combined_data/%s/NHANES_metadata_%s.tsv' % (year, year))
    return(pd.read_csv(datafile, sep='\t',
                       index_col=0, low_memory=False))


def open_dataset_page(dataset, year='2017-2018'):
    """
    open the web page describing a particular dataset
    - the dataset is listed in the Source variable with the metadata

    Parameters:
    -----------
    dataset: string, the code for the individual dataset
    year: the year code for the dataset

    """
    year_code = get_nhanes_year_code_dict()[year]
    url = 'https://wwwn.cdc.gov/Nchs/Nhanes/%s/%s_%s.htm' % (year, dataset, year_code)
    webbrowser.open(url)


def open_variable_page(variable, year='2017-2018'):
    """
    open the web page describing a particular dataset
    - the dataset is listed in the Source variable with the metadata

    Parameters:
    -----------
    dataset: string, the code for the individual dataset
    year: the year code for the dataset

    """
    metadata_df = load_NHANES_metadata(year)
    year_code = get_nhanes_year_code_dict()[year]
    varcode = metadata_df.loc[variable].Variable
    dataset = metadata_df.loc[variable].Source
    url = 'https://wwwn.cdc.gov/Nchs/Nhanes/%s/%s_%s.htm#%s' % (year, dataset, year_code, varcode)
    webbrowser.open(url)

"""
functions to load combined data
"""

import pkg_resources
import pandas as pd


def load_NHANES_data(year='2017-2018'):
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

    datafile = pkg_resources.resource_filename(
        'nhanes', 'combined_data/%s/NHANES_data_%s.tsv' % (year, year))
    return(pd.read_csv(datafile, sep='\t', index_col=0))


def load_NHANES_metadata(year='2017-2018'):
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
    datafile = pkg_resources.resource_filename(
        'nhanes', 'combined_data/%s/NHANES_metadata_%s.tsv' % (year, year))
    return(pd.read_csv(datafile, sep='\t', index_col=0))

import string
import os


def get_nhanes_year_code_dict(latest_year=2018):
    year_codes = {}
    year_letters = string.ascii_uppercase[1:]
    for index, year in enumerate(range(2001, latest_year, 2)):
        year_str = '%d-%d' % (year, year + 1)
        year_codes[year_str] = year_letters[index]
    return(year_codes)


def get_source_code_from_filepath(filepath):
    return(os.path.basename(filepath).split('_')[0])

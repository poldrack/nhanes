import string
import os
import json

datasets = [
    'HSQ', 'DBQ', 'DLQ', 'HIQ', 'SLQ', 'DPQ', 'SMQRTU',
    'PFQ', 'BPX', 'BMX', 'HDL', 'TCHOL',
    'PAQ', 'MCQ', 'DUQ', 'PBCD', 'DEMO',
    'DXX', 'DR1TOT', 'DR2TOT', 'DIQ', 'GHB',
    'SMQ', 'WHQ']

vars_to_keep = {
    'BMX': ['BMXWT', 'BMXRECUM', 'BMXHT', 'BMXBMI', 'BMXWAIST'],
    'BPX': ['BPXCHR', 'BPXPLS', 'BPXSY1', 'BPXDI1', 'BPXSY2', 'BPXDI2', 'BPXSY3', 'BPXDI3'],
    'DEMO': ['RIAGENDR', 'RIDAGEYR', 'RIDAGEMN', 'RIDRETH1', 'RIDRETH3', 'DMQMILIZ',
             'DMDEDUC3', 'DMDEDUC2', 'DMDMARTL', 'DMDHHSIZ', 'DMDFMSIZ', 'DMDHHSZA', 'DMDHHSZB',
             'DMDHHSZE', 'INDHHIN2', 'INDFMIN2', 'INDFMPIR'],
    'DIQ': ['DIQ010', 'DID040', 'DIQ160', 'DIQ170', 'DIQ175A', 'DIQ050', 'DIQ070', 'DIQ280', ],
    'DLQ': ['DLQ010', 'DLQ020', 'DLQ040', 'DLQ050', 'DLQ060', 'DLQ080', 'DLQ100', 'DLQ110',
            'DLQ140', 'DLQ150', ],
    'DPQ': ['DPQ010', 'DPQ020', 'DPQ030', 'DPQ040', 'DPQ050', 'DPQ060', 'DPQ070',
            'DPQ080', 'DPQ090', 'DPQ100'],
    'DR1TOT': ['DBD100', 'DRQSDIET', 'DRQSDT1', 'DRQSDT2', 'DRQSDT3', 'DRQSDT7',
               'DRQSDT9', 'DR1TKCAL', 'DR1TPROT', 'DR1TCARB', 'DR1TSUGR', 'DR1TFIBE',
               'DR1TTFAT', 'DR1TSFAT', 'DR1TMFAT', 'DR1TPFAT', 'DR1TCHOL',
               'DR1TALCO', 'DRD370B', ],
    'DR2TOT': ['DR2TKCAL', 'DR2TPROT', 'DR2TCARB', 'DR2TSUGR', 'DR2TFIBE',
               'DR2TTFAT', 'DR2TSFAT', 'DR2TMFAT', 'DR2TPFAT', 'DR2TCHOL',
               'DR2TALCO'],
    'DUQ': ['DUQ200', 'DUQ230', 'DUQ240', 'DUQ250', 'DUQ280', 'DUQ290', 'DUQ320',
            'DUQ330', 'DUQ360', 'DUQ430'],
    'DXX': ['DXXTRFAT', 'DXDTRPF', 'DXDTOPF'],
    'HDL': ['LBDHDD'],
    'HIQ': ['HIQ011', ],
    'HSQ': ['HSD010'],
    'MCQ': ['MCQ010', 'MCQ160a', 'MCQ160e', 'MCD180e', 'MCQ160f', 'MCD180f',
            'MCQ220', 'MCQ230a', 'MCD240a'],
    'PAQ': ['PAQ605', 'PAQ620', 'PAQ635', 'PAQ650', 'PAQ665', 'PAD680', ],
    'PBCD': ['LBXBPB', 'LBXBCD', 'LBXTHG', 'LBXBSE', 'LBXBMN', ],
    'PFQ': ['PFQ057', 'PFQ059', 'PFQ061A'],
    'SLQ': ['SLQ300', 'SLQ310', 'SLD012', 'SLQ030', 'SLQ050', 'SLQ120'],
    'SMQRTU': ['SMQ681', 'SMQ710', 'SMQ720', 'SMDANY'],
    'SMQ': ['SMQ020', 'SMD030', 'SMQ040', 'SMD641', 'SMD650', 'SMQ670', 'SMQ848',
            'SMQ852Q', 'SMQ852U'],
    'TCHOL': ['LBXTC'],
    'WHQ': ['WHD010', 'WHD020', 'WHQ070', 'WHQ225']
}


def get_vars_to_keep(infile='vars_to_keep.json'):
    with open(infile, 'r') as f:
        vars_to_keep = json.load(f)
    # make everythign uppercase to match pandas
    for dataset in vars_to_keep:
        vars_to_keep[dataset] = [i.upper() for i in vars_to_keep[dataset]]
    return(vars_to_keep)


def get_datasets(infile='datasets.json'):
    with open(infile, 'r') as f:
        return(json.load(f))


def get_nhanes_year_code_dict(latest_year=2018):
    year_codes = {}
    year_letters = string.ascii_uppercase[1:]
    for index, year in enumerate(range(2001, latest_year, 2)):
        year_str = '%d-%d' % (year, year + 1)
        year_codes[year_str] = year_letters[index]
    return(year_codes)


def get_source_code_from_filepath(filepath):
    return(os.path.basename(filepath).split('_')[0])


class EmptySectionError(Exception):
    pass


def make_long_variable_name(label):
    return(''.join([i.title() for i in label.translate(
        str.maketrans('', '', string.punctuation)).split(' ')]))

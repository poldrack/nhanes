# nhanes
Python interface to NHANES dataset

## What is NHANES?

NHANES is the [National Health and Nutrition Examination Survey](https://www.cdc.gov/nchs/nhanes/index.htm), which is run by the US Centers for Disease Control. Every year, the study examines a representative sample of about 5000 individuals from across the United States, using a broad range of surveys, physiological measurements, and laboratory tests.  These data are useful for many purposes --- the main interest of the developer is to use them in teaching introductory statistics.  There is a R package that provides access to the NHANES dataset, and the present package is meant to provide similar access to python users.

## Installing the package

You can install the package using the following command:

```pip install nhanes```

Currently the dataset gives access to data from the most recent release at the time of development, which is the 2017-2018 data release.  

## Using the package

The package provides both a selected subset of the data, as well as detailed metadata regarding the selected variables, both stored as pandas data frames.  To access the data and metadata, use the following code:

```
from nhanes.load import load_NHANES_data, load_NHANES_metadata

data_df = load_NHANES_data(year='2017-2018')
metadata_df = load_NHANES_metadata(year='2017-2018')
```

The row indices in the metadata match the column names in the data:

```
In [10]: data_df['GeneralHealthCondition']
Out[10]:
SEQN
93703.0           NaN
93704.0           NaN
93705.0          Good
93706.0     Very good
93707.0          Good
              ...
102952.0    Very good
102953.0         Fair
102954.0         Good
102955.0    Very good
102956.0         Good
Name: GeneralHealthCondition, Length: 8366, dtype: object

In [11]: metadata_df.loc['GeneralHealthCondition']
Out[11]:
Variable                                                                   HSD010
Type                                                                      Numeric
Format                                                                        NaN
Informat                                                                      NaN
Label                                                    General health condition
Source                                                                        HSQ
VariableName                                                               HSD010
SASLabel                                                 General health condition
EnglishText                     Next I have some general questions about {your...
Target                           Both males and females 12 YEARS -\n\n\t\t\t15...
VariableNameLong_variable_df                               GeneralHealthCondition
Source_variable_df                                                            HSQ
EnglishInstructions                                                           NaN
HardEdits                                                                     NaN
Recoded                                                                      True
Name: GeneralHealthCondition, dtype: object

```


## Building our own data

A script called ``make_combined_NHANES_data.py`` is provided so that you can recreate the data for different releases and using different variable sets.
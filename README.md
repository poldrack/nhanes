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

## Building our own data

A script called ``make_combined_NHANES_data.py`` is provided so that you can recreate the data for different releases and using different variable sets.
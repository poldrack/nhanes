from nhanes.load import load_NHANES_data, load_NHANES_metadata


def test_load_data():
    df = load_NHANES_data(year='2017-2018')
    assert df.shape[0] == 8366
    assert df.shape[1] == 197


def test_load_metadata():
    df = load_NHANES_metadata(year='2017-2018')
    assert df.shape[0] == 865
    assert df.shape[1] == 15


def test_match():
    data_df = load_NHANES_data(year='2017-2018')
    metadata_df = load_NHANES_metadata(year='2017-2018')
    # make sure column names in data match index in metadata exactly
    assert not set(data_df.columns).difference(metadata_df.index)

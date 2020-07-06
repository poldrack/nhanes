from nhanes.load import load_NHANES_data, load_NHANES_metadata

def test_load_data():
    df = load_NHANES_data(year='2017-2018')
    assert df.shape[0] == 8366
    assert df.shape[1] == 197
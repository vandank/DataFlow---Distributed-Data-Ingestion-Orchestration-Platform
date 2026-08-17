import pandas as pd

from app.services.storage.dataframe_storage import DataFrameStorageService


def test_dataframe_round_trip():
    storage = DataFrameStorageService()

    original = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
    )

    object_name = "test/round_trip.csv"

    storage.write_csv(
        df=original,
        object_name=object_name,
    )

    restored = storage.read_csv(object_name)

    pd.testing.assert_frame_equal(
        original,
        restored,
    )
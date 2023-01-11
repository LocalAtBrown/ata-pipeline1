"""
This is a dummy function and maybe even a dummy file (almost definitely will be renamed, at least).
Placeholder for "do things with event data and spit out prescriptions".
"""
from datetime import datetime

import pandas as pd


def process(events: pd.DataFrame) -> pd.DataFrame:
    # do something mindless as a placeholder
    r, _ = events.shape

    prescribe = [True] * r
    last_updated = [datetime.now()] * r

    data = {"prescribe": prescribe, "last_updated": last_updated}

    return pd.DataFrame(data=data)

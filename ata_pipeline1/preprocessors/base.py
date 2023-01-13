from abc import ABC, abstractmethod

import pandas as pd


class Preprocessor(ABC):
    """
    Base preprocessor abstract class. Its children should be dataclasses storing
    variables needed for the specific transformation.
    """

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calls an instance of a child class as if it's a (preprocessing) function.
        """
        df_out = self.transform(df)
        self.log_result(df, df_out)
        return df_out

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms a Snowplow DataFrame using parameters predefined in the dataclass.
        """
        pass

    @abstractmethod
    def log_result(self, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
        """
        Logs useful post-transformation messages.
        """
        pass

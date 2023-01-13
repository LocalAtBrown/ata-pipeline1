from dataclasses import dataclass
from typing import Callable, Dict, Tuple, Union

import pandas as pd

from ata_pipeline1.helpers.fields import Field, FieldNew, FieldPreAgg
from ata_pipeline1.preprocessors.base import Preprocessor


@dataclass
class AggregatePageActivities(Preprocessor):
    """
    Combine events of the same parent and creates (or replaces existing fields with)
    new fields with aggregation/summary statistics (e.g., max scroll depth, dwell time).
    """

    agg_funcs: Dict[Field, Tuple[FieldPreAgg, Union[Callable, str]]]
    field_event_parent_id: FieldNew = FieldNew.EVENT_PARENT_ID

    # TODO: When aggregating, create a new column showing index of visit within session
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby(self.field_event_parent_id).aggregate(**self._create_named_agg_objects())

    def _create_named_agg_objects(self) -> Dict[Field, pd.NamedAgg]:
        return {
            field_agg: pd.NamedAgg(column=field_basis, aggfunc=func)
            for field_agg, (field_basis, func) in self.agg_funcs.items()
        }

    def log_result(self, df_in, df_out) -> None:
        pass

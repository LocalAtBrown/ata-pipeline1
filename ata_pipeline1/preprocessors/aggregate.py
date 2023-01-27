from dataclasses import dataclass
from typing import Callable, Dict, Tuple, Union

import pandas as pd

from ata_pipeline1.helpers.enums import FieldNew, FieldSnowplow
from ata_pipeline1.helpers.logging import logging
from ata_pipeline1.helpers.typing import Field
from ata_pipeline1.preprocessors.base import Preprocessor

logger = logging.getLogger(__name__)


@dataclass
class AggregatePageActivities(Preprocessor):
    """
    Combine events of the same parent and creates (or replaces existing fields with)
    new fields with aggregation/summary statistics (e.g., max scroll depth, dwell time).
    """

    agg_funcs: Dict[Field, Tuple[Field, Union[Callable, str]]]
    field_event_id: FieldSnowplow = FieldSnowplow.EVENT_ID
    field_event_parent_id: FieldNew = FieldNew.EVENT_PARENT_ID

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df_agg = df.groupby(self.field_event_parent_id).aggregate(**self._create_named_agg_objects())
        # Rename index name from event_parent_id to event_id
        df_agg.index.names = [self.field_event_id]
        return df_agg

    def _create_named_agg_objects(self) -> Dict[Field, pd.NamedAgg]:
        return {
            field_agg: pd.NamedAgg(column=field_basis, aggfunc=func)
            for field_agg, (field_basis, func) in self.agg_funcs.items()
        }

    def log_result(self, df_in, df_out) -> None:
        logger.info("Aggregated page activities into page views with summary statistics")

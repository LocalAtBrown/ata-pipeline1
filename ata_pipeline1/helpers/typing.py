from typing import Union

from typing_extensions import TypeAlias

from ata_pipeline1.helpers.enums import FieldNew, FieldSnowplow

Field: TypeAlias = Union[FieldNew, FieldSnowplow]

from typing import List

import pytest

from ata_pipeline1.helpers.enums import FieldNew, FieldSnowplow
from ata_pipeline1.helpers.typing import Field
from ata_pipeline1.preprocessors import ConvertFieldTypes


@pytest.fixture(scope="package")
def fields() -> List[Field]:
    return [
        FieldSnowplow.DERIVED_TSTAMP,
        FieldSnowplow.DOC_HEIGHT,
        FieldSnowplow.DOMAIN_SESSIONIDX,
        FieldSnowplow.DOMAIN_USERID,
        FieldSnowplow.DVCE_SCREENHEIGHT,
        FieldSnowplow.DVCE_SCREENWIDTH,
        FieldSnowplow.EVENT_ID,
        FieldSnowplow.EVENT_NAME,
        FieldSnowplow.NETWORK_USERID,
        FieldSnowplow.PAGE_REFERRER,
        FieldSnowplow.PAGE_URLPATH,
        FieldSnowplow.PP_YOFFSET_MAX,
        FieldSnowplow.REFR_MEDIUM,
        FieldSnowplow.REFR_SOURCE,
        FieldNew.SITE_NAME,
        FieldSnowplow.SEMISTRUCT_FORM_CHANGE,
        FieldSnowplow.SEMISTRUCT_FORM_FOCUS,
        FieldSnowplow.SEMISTRUCT_FORM_SUBMIT,
        FieldSnowplow.USERAGENT,
    ]


@pytest.fixture(scope="package")
def preprocessor_convert_all_field_types() -> ConvertFieldTypes:
    return ConvertFieldTypes()


@pytest.fixture(scope="package")
def dummy_email() -> str:
    return "dummy@dummydomain.com"

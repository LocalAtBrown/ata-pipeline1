from typing import Any

import pandas as pd
import pytest

from ata_pipeline1.helpers.enums import EventName, FieldSnowplow
from ata_pipeline1.site.names import SiteName
from ata_pipeline1.site.newsletter.open_vallejo import COMPONENT_ZERO


@pytest.mark.unit
class TestComponentZero:
    """
    Unit tests for original newsletter-signup validation component.
    """

    @pytest.fixture(scope="class")
    def event_inline(self, fields, preprocessor_convert_all_field_types, dummy_email) -> "pd.Series[Any]":
        df = pd.DataFrame(
            [
                [
                    "2022-11-04 17:04:17.234",
                    "4284",
                    "2",
                    "f7a02cc7-e9f1-40bc-99ca-9d1aad111070",
                    "693",
                    "320",
                    "dc2295e6-7e64-4f17-9df3-53466045eac9",
                    EventName.SUBMIT_FORM,
                    "173295ed-93ac-4504-8dd8-d880f8d2c74e",
                    "https://openvallejo.org/donate/?mc_cid=396eb8ce49&mc_eid=3327a7bf29&utm_campaign=396eb8ce49-Vallejo+patrol+staffing+story_COPY_01&utm_medium=email&utm_source=Open+Vallejo&utm_term=0_5c634b5220-396eb8ce49-600953573",
                    "/newsletter/",
                    None,
                    "internal",
                    None,
                    SiteName.OPEN_VALLEJO,
                    None,
                    None,
                    "{'formId': 'mc-embedded-subscribe-form', 'formClasses': ['validate'], 'elements': [{'name': 'EMAIL', 'value': '%s', 'nodeName': 'INPUT', 'type': 'email'}, {'name': 'b_625f546ae539f2396949b95f4_5c634b5220', 'value': '', 'nodeName': 'INPUT', 'type': 'text'}]}"
                    % dummy_email,
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6,2 Mobile/15E148 Safari/604.1",
                ]
            ],
            columns=fields,
        )

        return preprocessor_convert_all_field_types(df).iloc[0]

    @pytest.fixture(scope="class")
    def event_popup(self) -> "pd.Series[Any]":
        # TODO once we have data for this kind of form in S3
        pass

    def test_is_newsletter_inline_form_true(self, event_inline) -> None:
        assert COMPONENT_ZERO.is_newsletter_inline_form(event_inline)

    def test_is_newsletter_inline_form_false(self, event_inline, dummy_email) -> None:
        event_inline = event_inline.copy()
        event_inline[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] = {
            "formId": "dummy-id",  # must include the "mc-embedded-subscribe-form" string
            "formClasses": [],
            "elements": [
                {
                    "name": "email",
                    "value": dummy_email,
                    "nodeName": "INPUT",
                    "type": "email",
                }
            ],
        }
        assert COMPONENT_ZERO.is_newsletter_inline_form(event_inline) is False

    def test_is_newsletter_popup_form_true(self) -> None:
        # TODO once we have data for this kind of form in S3
        pass

    def test_is_newsletter_popup_form_false(self) -> None:
        # TODO once we have data for this kind of form in S3
        pass

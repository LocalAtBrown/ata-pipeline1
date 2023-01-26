from typing import Any

import pandas as pd
import pytest

from ata_pipeline1.helpers.enums import EventName, FieldSnowplow
from ata_pipeline1.site.names import SiteName
from ata_pipeline1.site.newsletter.the_19th import COMPONENT_ZERO


@pytest.mark.unit
class TestComponentZero:
    """
    Unit tests for original newsletter-signup validation component.
    """

    @pytest.fixture(scope="class")
    def event(self, fields, preprocessor_convert_all_field_types, dummy_email) -> "pd.Series[Any]":
        df = pd.DataFrame(
            [
                [
                    "2022-11-07 17:31:18.216",
                    "6991",
                    "1",
                    "7434e1bd-3539-42fa-948d-96801d807476",
                    "1024",
                    "768",
                    "4d768aef-addc-4a3a-a133-1ea7fbd184f1",
                    EventName.SUBMIT_FORM,
                    "b800111b-fe37-4f49-b0c9-61fbbcc5ff4a",
                    "https://www.google.com/",
                    "/2022/06/mayra-flores-record-women-congress/",
                    None,
                    "search",
                    "Google",
                    SiteName.THE_19TH,
                    None,
                    None,
                    "{'formId': 'newsletter-form-block_62b24d43296c5', 'formClasses': ['newsletter-form', 'align', 'js-newsletter-form'], 'elements': [{'name': 'EMAIL', 'value': '%s', 'nodeName': 'INPUT', 'type': 'email'}, {'name': 'subscribe-confirmation-block_62b24d43296c5', 'value': 'on', 'nodeName': 'INPUT', 'type': 'checkbox'}, {'name': 'NEWSLETTER', 'value': 'e35d7d0333', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'group[70588][1]', 'value': '1', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'b_8c4d626920c5131bb82226529_a35c3279be', 'value': '', 'nodeName': 'INPUT', 'type': 'text'}]}"
                    % dummy_email,
                    "Mozilla/5.0 (iPad; CPU OS 11_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.0 Mobile/15E148 Safari/604.1",
                ]
            ],
            columns=fields,
        )

        return preprocessor_convert_all_field_types(df).iloc[0]

    def test_is_newsletter_form_true(self, event) -> None:
        assert COMPONENT_ZERO.is_newsletter_form(event)

    def test_is_newsletter_form_false(self, event, dummy_email) -> None:
        event = event.copy()
        event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] = {
            "formId": "dummy-id",  # must include the "newsletter" string
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
        assert COMPONENT_ZERO.is_newsletter_form(event) is False

from typing import Any

import pandas as pd
import pytest

from ata_pipeline1.helpers.enums import EventName, FieldSnowplow
from ata_pipeline1.site.names import SiteName
from ata_pipeline1.site.newsletter.dallas_free_press import COMPONENT_ZERO


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
                    "2022-11-04 15:33:57.85",
                    "1502",
                    "1",
                    "a1ee0745-f471-4695-8b3d-a2320587f5ed",
                    "800",
                    "1280",
                    "8c62f083-539a-4158-bddd-ec83271639cd",
                    EventName.SUBMIT_FORM,
                    "957e4dc1-2754-4213-949b-d5928d446911",
                    "https://dallasfreepress.com/dallas-news/dallas-forgot-remember-reclaim-lost-black-schools-education-history/",
                    "/text-and-email-notifications/",
                    None,
                    "internal",
                    None,
                    SiteName.DALLAS_FREE_PRESS,
                    None,
                    None,
                    "{'formId': 'mc-embedded-subscribe-form', 'formClasses': ['validate'], 'elements': [{'name': 'EMAIL', 'value': '%s', 'nodeName': 'INPUT', 'type': 'email'}, {'name': 'FNAME', 'value': 'Libby', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'LNAME', 'value': 'Daniels', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'MMERGE3', 'value': '75231', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'MMERGE4', 'value': '9729253923', 'nodeName': 'INPUT', 'type': 'text'}, {'name': 'group[8368][1]', 'value': '1', 'nodeName': 'INPUT', 'type': 'checkbox'}, {'name': 'group[8368][2]', 'value': '2', 'nodeName': 'INPUT', 'type': 'checkbox'}, {'name': 'group[8368][4]', 'value': None, 'nodeName': 'INPUT', 'type': 'checkbox'}, {'name': 'b_6cbf3c038f5cc4d279f4da4ed_37f4ad3cfe', 'value': '', 'nodeName': 'INPUT', 'type': 'text'}]}"
                    % dummy_email,
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
                ]
            ],
            columns=fields,
        )

        return preprocessor_convert_all_field_types(df).iloc[0]

    def test_is_newsletter_inline_form_true(self, event) -> None:
        assert COMPONENT_ZERO.is_newsletter_inline_form(event)

    def test_is_newsletter_inline_form_false(self, event, dummy_email) -> None:
        event = event.copy()
        event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] = {
            "formId": "dummy-id",  # must be "mc-embedded-subscribe-form"
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
        assert COMPONENT_ZERO.is_newsletter_inline_form(event) is False

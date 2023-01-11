from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from ata_pipeline1.helpers.fields import FieldSnowplow


# ---------- SITE FORM-SUBMISSION EVENT UTILITIES ----------
@dataclass
class FormElement:
    """
    JSON data schema of an element as part of Snowplow form-submission event data
    (see: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    """

    # These constraints are looser than what the schema requires. If we need
    # them to be stricter, consider pydantic.
    name: str
    node_name: str
    value: Optional[str] = None
    type: Optional[str] = None


@dataclass
class FormSubmitData:
    """
    JSON data schema of a Snowplow form-submission event
    (see: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    """

    # These constraints are looser than what the schema requires. If we need
    # them to be stricter, consider pydantic.
    form_id: str
    form_classes: List[str]
    elements: List[FormElement]


def parse_form_submit_dict(data: Dict) -> FormSubmitData:
    """
    Creates a dataclass from a corresponding `dict` of form-submission data.
    """
    return FormSubmitData(
        form_id=data["formId"],
        form_classes=data["formClasses"],
        elements=[
            FormElement(name=e["name"], node_name=e["nodeName"], value=e.get("value"), type=e.get("type"))
            for e in data["elements"]
        ],
    )


# ---------- SITE NEWSLETTER-FORM-SUBMISSION VALIDATORS ----------
class SiteNewsletterSignupValidator(ABC):
    """
    Base class storing common newsletter-form-submission validators across all of our
    partners.
    """

    @staticmethod
    def has_data(event: pd.Series) -> bool:
        """
        Checks if a form-submission event actually has form HTML data.
        """
        # Should only be either dict or None because we'll perform this check
        # after the ConvertFieldTypes and ReplaceNaNs preprocessors
        return event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] is not None

    @staticmethod
    def has_email_input(event: pd.Series) -> bool:
        """
        Checks if the HTML form of a form-submission event has an `<input type="email">`
        element, which is the case in all of our partners' newsletter forms.
        """
        form_data = parse_form_submit_dict(event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return any([e.node_name == "INPUT" and e.type == "email" for e in form_data.elements])

    @abstractmethod
    def validate(self, event: pd.Series) -> bool:
        """
        Main validation method.

        Checks if a form-submission event is of a newsletter form using a pre-specified
        list of individual validators. If one validator fails, it automatically fails.
        """
        return self.has_data(event) and self.has_email_input(event)


class AfroLaNewsletterSignupValidator(SiteNewsletterSignupValidator):
    """
    Newsletter-form-submission validation logic for AfroLA.
    """

    @staticmethod
    def is_in_newsletter_page(event: pd.Series) -> bool:
        """
        Checks if the URL path where the form submission happens is correct.
        """
        return event[FieldSnowplow.PAGE_URLPATH] == "/subscribe"

    def validate(self, event: pd.Series) -> bool:
        return super().validate(event) and self.is_in_newsletter_page(event)


class DallasFreePressNewsletterSignupValidator(SiteNewsletterSignupValidator):
    """
    Newsletter-form-submission validation logic for DFP.
    """

    @staticmethod
    def is_newsletter_inline_form(event: pd.Series) -> bool:
        """
        Checks if the HTML form is an inline Mailchimp newsletter form.
        """
        form_data = parse_form_submit_dict(event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return form_data.form_id == "mc-embedded-subscribe-form"

    def validate(self, event: pd.Series) -> bool:
        return super().validate(event) and self.is_newsletter_inline_form(event)


class OpenVallejoNewsletterSignupValidator(SiteNewsletterSignupValidator):
    """
    Newsletter-form-submission validation logic for OpenVallejo.
    """

    @staticmethod
    def is_newsletter_inline_form(event: pd.Series) -> bool:
        """
        Checks if the HTML form is an inline Mailchimp newsletter form.
        """
        form_data = parse_form_submit_dict(event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return form_data.form_id == "mc-embedded-subscribe-form"

    @staticmethod
    def is_newsletter_popup_form(event: pd.Series) -> bool:
        """
        Checks if the HTML form is a pop-up Mailchimp newsletter form.
        """

        # TODO once we have data for this kind of form in S3
        pass

    def validate(self, event: pd.Series) -> bool:
        return super().validate(event) and (
            self.is_newsletter_inline_form(event) or self.is_newsletter_popup_form(event)
        )


class The19thNewsletterSignupValidator(SiteNewsletterSignupValidator):
    """
    Newsletter-form-submission validation logic for The 19th.
    """

    @staticmethod
    def is_newsletter_form(event: pd.Series) -> bool:
        """
        Checks if the HTML form is a newsletter form via its ID.
        """
        form_data = parse_form_submit_dict(event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return "newsletter" in form_data.form_id

    def validate(self, event: pd.Series) -> bool:
        return super().validate(event) and self.is_newsletter_form(event)

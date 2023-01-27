from typing import Any

import pandas as pd

from ata_pipeline1.helpers.datetime import TIMESTAMP_POSIX
from ata_pipeline1.helpers.enums import FieldSnowplow
from ata_pipeline1.site.newsletter.base import (
    SiteNewsletterSignupValidator,
    SiteNewsletterSignupValidatorComponent,
    parse_form_submit_dict,
)


class ComponentZero(SiteNewsletterSignupValidatorComponent):
    """
    Original newsletter-form-submission validation logic.
    """

    @staticmethod
    def is_newsletter_inline_form(event: "pd.Series[Any]") -> bool:
        """
        Checks if the HTML form is an inline Mailchimp newsletter form.
        """
        form_data = parse_form_submit_dict(event[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT])
        return form_data.form_id == "mc-embedded-subscribe-form"

    def validate(self, event: "pd.Series[Any]") -> bool:
        return self.has_data(event) and self.has_email_input(event) and self.is_newsletter_inline_form(event)


COMPONENT_ZERO = ComponentZero(effective_starting=TIMESTAMP_POSIX)

# When Dallas Free Press rules change, add a new component here (create a new child class of
# SiteNewsletterSignupValidatorComponent as necessary) and add it to the list of components in VALIDATOR

VALIDATOR = SiteNewsletterSignupValidator(components=[COMPONENT_ZERO])

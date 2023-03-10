from typing import Any

import pandas as pd

from ata_pipeline1.helpers.dataclasses import FormSubmitData
from ata_pipeline1.helpers.datetime import TIMESTAMP_POSIX
from ata_pipeline1.helpers.enums import FieldSnowplow
from ata_pipeline1.site.newsletter.base import (
    SiteNewsletterSignupValidator,
    SiteNewsletterSignupValidatorComponent,
)


class ComponentZero(SiteNewsletterSignupValidatorComponent):
    """
    Original newsletter-form-submission validation logic.
    """

    @staticmethod
    def is_newsletter_form(event: "pd.Series[Any]") -> bool:
        """
        Checks if the HTML form is a newsletter form via its ID.
        """
        form_data: FormSubmitData = event.at[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT]
        return "newsletter" in form_data.form_id

    def validate(self, event: "pd.Series[Any]") -> bool:
        return self.has_data(event) and self.has_email_input(event) and self.is_newsletter_form(event)


COMPONENT_ZERO = ComponentZero(effective_starting=TIMESTAMP_POSIX)

# When The 19th* rules change, add a new component here (create a new child class of
# SiteNewsletterSignupValidatorComponent as necessary) and add it to the list of components in VALIDATOR

VALIDATOR = SiteNewsletterSignupValidator(components=[COMPONENT_ZERO])

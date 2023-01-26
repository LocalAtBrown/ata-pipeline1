from typing import Any

import pandas as pd

from ata_pipeline1.helpers.datetime import TIMESTAMP_POSIX
from ata_pipeline1.site.newsletter.base import (
    SiteNewsletterSignupValidator,
    SiteNewsletterSignupValidatorComponent,
)
from ata_pipeline1.site.page import PAGE_CLASSIFIER_AFRO_LA


class ComponentZero(SiteNewsletterSignupValidatorComponent):
    """
    Original newsletter-form-submission validation logic.
    """

    @staticmethod
    def is_in_newsletter_page(event: "pd.Series[Any]") -> bool:
        """
        Checks if the URL path where the form submission happens is correct.

        Need to do this because
        """
        return PAGE_CLASSIFIER_AFRO_LA.is_newsletter(event)

    def validate(self, event: "pd.Series[Any]") -> bool:
        return self.has_data(event) and self.has_email_input(event) and self.is_in_newsletter_page(event)


COMPONENT_ZERO = ComponentZero(effective_starting=TIMESTAMP_POSIX)

VALIDATOR = SiteNewsletterSignupValidator(components=[COMPONENT_ZERO])

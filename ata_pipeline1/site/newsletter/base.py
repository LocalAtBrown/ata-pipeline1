from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List

import pandas as pd

from ata_pipeline1.helpers.dataclasses import FormSubmitData
from ata_pipeline1.helpers.datetime import (
    AppliesDuringTimePeriod,
    ChangesBetweenTimePeriods,
)
from ata_pipeline1.helpers.enums import FieldSnowplow


class NewsletterSignupValidator(ABC):
    """
    Base class for a site's rule-based newsletter-signup validator.
    """

    @abstractmethod
    def validate(self, event: "pd.Series[Any]") -> bool:
        """
        Main validation method.

        Checks if a form-submission event is of a newsletter form using a pre-specified
        list of individual validators. If one validator fails, it automatically fails.
        """
        pass


class SiteNewsletterSignupValidatorComponent(NewsletterSignupValidator, AppliesDuringTimePeriod):
    """
    Base class of a newsletter-signup validator component whose rules only
    applies from some particular point in time.

    Feel free to subclass this class and override methods as needed to create
    components with more complex validation rules.
    """

    def __init__(self, effective_starting: datetime) -> None:
        self.set_effective_starting(effective_starting)

    @staticmethod
    def has_data(event: "pd.Series[Any]") -> bool:
        """
        Checks if a form-submission event actually has form HTML data.
        """
        # Should only be either dict or None because we'll perform this check
        # after the ConvertFieldTypes and ReplaceValues([np.nan], [None]) preprocessors
        return event.at[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT] is not None

    @staticmethod
    def has_email_input(event: "pd.Series[Any]") -> bool:
        """
        Checks if the HTML form of a form-submission event has an `<input type="email">`
        element, which is the case in all of our partners' newsletter forms.
        """
        form_data: FormSubmitData = event.at[FieldSnowplow.SEMISTRUCT_FORM_SUBMIT]
        return any([e.node_name == "INPUT" and e.type == "email" for e in form_data.elements])


class SiteNewsletterSignupValidator(NewsletterSignupValidator, ChangesBetweenTimePeriods):
    """
    Base class of a newsletter-signup validator that consists of one or more
    validator components.
    """

    def __init__(self, components: List[SiteNewsletterSignupValidatorComponent]) -> None:
        self.set_components(components)

    def validate(self, event: "pd.Series[Any]") -> bool:
        component: SiteNewsletterSignupValidatorComponent = self.assign_component(
            event.at[FieldSnowplow.DERIVED_TSTAMP]
        )
        return component.validate(event)

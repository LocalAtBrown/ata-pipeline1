from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

import pandas as pd

from ata_pipeline1.helpers.enums import FieldSnowplow
from ata_pipeline1.helpers.mixins import AppliesFromTimestamp, ChangesBetweenTimestamps


class PageClassifier(ABC):
    """
    Base class for a site's rule-based, multilabel, page-type classifier.
    """

    def __init__(self, *args, **kwargs) -> None:
        # Formality to make sure the next class after this one in the MRO of
        # whichever class that subclasses this one has its __init__ method called
        # (see: https://www.youtube.com/watch?v=X1PQ7zzltz4, 10:00 mark)
        super().__init__(*args, **kwargs)

    @abstractmethod
    def is_home(self, event: pd.Series) -> bool:
        """
        Checks if page is home page.
        """
        pass

    @abstractmethod
    def is_about_us(self, event: pd.Series) -> bool:
        """
        Checks if page is an "About us" page, or an "Our team" page, or something
        similar.
        """
        pass

    @abstractmethod
    def is_newsletter(self, event: pd.Series) -> bool:
        """
        Checks if page is a newsletter-form page (e.g., https://19thnews.org/newsletters/daily/).

        A good way to determine if a page is a newsletter page is to consider whether
        making someone subscribe to a newsletter(s) is a primary (or even the only) reason
        that said page exists. Under this criterion, a story or article that has a
        built-in newsletter form shouldn't be considered a newsletter page.
        """
        pass

    @abstractmethod
    def is_donation(self, event: pd.Series) -> bool:
        """
        Checks if page is a donation or membership-signup page (e.g., https://dallasfreepress.com/support-dfp/).

        A good way to determine if a page is a donation page is to consider whether
        asking someone to donate is a primary (or even the only) reason
        that said page exists. Under this criterion, a story or article that has a
        built-in donation form shouldn't be considered a donation page.
        """
        pass

    @abstractmethod
    def is_article(self, event: pd.Series) -> bool:
        """
        Checks if page is an article or story.
        """
        pass

    @abstractmethod
    def is_section(self, event: pd.Series) -> bool:
        """
        Checks if page is a section or topic page.
        """
        pass

    @abstractmethod
    def is_author_profile(self, event: pd.Series) -> bool:
        """
        Checks if page is author profile and eheir list of articles.
        """
        pass


class SitePageClassifierComponent(PageClassifier, AppliesFromTimestamp):
    """
    Base class of a classifier component whose rules only applies from
    some particular point in time.
    """

    def __init__(self, effective_starting: datetime = datetime(1970, 1, 1)) -> None:
        super().__init__(effective_starting=effective_starting)


class SitePageClassifier(PageClassifier, ChangesBetweenTimestamps):
    """
    Base class of a classifier that consists of one or more classifier
    components.
    """

    def __init__(self, components: List[SitePageClassifierComponent]) -> None:
        super().__init__(components=components)

    def assign_component(self, event: pd.Series) -> SitePageClassifierComponent:
        return super().assign_component(event[FieldSnowplow.DERIVED_TSTAMP])

    def is_home(self, event: pd.Series) -> bool:
        return self.assign_component(event).is_home(event)

    def is_about_us(self, event: pd.Series) -> bool:
        return self.assign_component(event).is_about_us(event)

    def is_newsletter(self, event: pd.Series) -> bool:
        return self.assign_component(event).is_newsletter(event)

    def is_donation(self, event: pd.Series) -> bool:
        return self.assign_component(event).is_donation(event)

    def is_article(self, event: pd.Series) -> bool:
        return self.assign_component(event).is_article(event)

    def is_section(self, event: pd.Series) -> bool:
        return self.assign_component(event).is_section(event)

    def is_author_profile(self, event: pd.Series) -> bool:
        return self.assign_component(event).is_author_profile(event)

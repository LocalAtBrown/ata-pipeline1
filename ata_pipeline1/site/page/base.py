import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List

import pandas as pd

from ata_pipeline1.helpers.enums import FieldSnowplow
from ata_pipeline1.helpers.mixins import (
    AppliesDuringTimePeriod,
    ChangesBetweenTimePeriods,
)

# URL-path antipattern string that will definitely never match anything inside an
# actual URL
URLPATH_ANTIPATTERN = r"\s<>%{}`"


@dataclass(frozen=True)
class Patterns:
    home: re.Pattern[str]
    about_us: re.Pattern[str]
    newsletter: re.Pattern[str]
    donation: re.Pattern[str]
    article: re.Pattern[str]
    section: re.Pattern[str]
    author_profile: re.Pattern[str]


class PageClassifier(ABC):
    """
    Base class for a site's rule-based, multilabel, page-type classifier.
    """

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
        Checks if page is a section, topic, or tag page.
        """
        pass

    @abstractmethod
    def is_author_profile(self, event: pd.Series) -> bool:
        """
        Checks if page is author profile and their list of articles.
        """
        pass


class SitePageClassifierComponent(PageClassifier, AppliesDuringTimePeriod):
    """
    Base class of a classifier component whose rules only applies from
    some particular point in time.

    This default component only performs a regex match of an event's `page_urlpath`
    to determine the page type (regex patterns are supplied by the programmer).
    If you need to perform more complex operations to determine page type,
    create a new class that subclasses this class and override methods as needed.
    """

    def __init__(
        self,
        effective_starting: datetime,
        use_default_patterns_schema: bool = True,
        home: str = URLPATH_ANTIPATTERN,
        about_us: str = URLPATH_ANTIPATTERN,
        newsletter: str = URLPATH_ANTIPATTERN,
        donation: str = URLPATH_ANTIPATTERN,
        article: str = URLPATH_ANTIPATTERN,
        section: str = URLPATH_ANTIPATTERN,
        author_profile: str = URLPATH_ANTIPATTERN,
    ) -> None:
        self.set_effective_starting(effective_starting)

        if use_default_patterns_schema:
            self.patterns = Patterns(
                home=re.compile(home),
                about_us=re.compile(about_us),
                newsletter=re.compile(newsletter),
                donation=re.compile(donation),
                article=re.compile(article),
                section=re.compile(section),
                author_profile=re.compile(author_profile),
            )

    def is_home(self, event: pd.Series) -> bool:
        return bool(self.patterns.home.search(event[FieldSnowplow.PAGE_URLPATH]))

    def is_about_us(self, event: pd.Series) -> bool:
        return bool(self.patterns.about_us.search(event[FieldSnowplow.PAGE_URLPATH]))

    def is_newsletter(self, event: pd.Series) -> bool:
        return bool(self.patterns.newsletter.search(event[FieldSnowplow.PAGE_URLPATH]))

    def is_donation(self, event: pd.Series) -> bool:
        return bool(self.patterns.donation.search(event[FieldSnowplow.PAGE_URLPATH]))

    def is_article(self, event: pd.Series) -> bool:
        return bool(self.patterns.article.search(event[FieldSnowplow.PAGE_URLPATH]))

    def is_section(self, event: pd.Series) -> bool:
        return bool(self.patterns.section.search(event[FieldSnowplow.PAGE_URLPATH]))

    def is_author_profile(self, event: pd.Series) -> bool:
        return bool(self.patterns.author_profile.search(event[FieldSnowplow.PAGE_URLPATH]))


class SitePageClassifier(PageClassifier, ChangesBetweenTimePeriods):
    """
    Base class of a classifier that consists of one or more classifier
    components.
    """

    def __init__(self, components: List[SitePageClassifierComponent]) -> None:
        self.set_components(components)

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

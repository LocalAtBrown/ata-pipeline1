import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, cast

import pandas as pd

from ata_pipeline1.helpers.datetime import (
    AppliesDuringTimePeriod,
    ChangesBetweenTimePeriods,
)
from ata_pipeline1.helpers.enums import FieldSnowplow, PageType
from ata_pipeline1.helpers.re import ANTIPATTERN_URLPATH


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
    def perform_common_operation(self, event: pd.Series, page_type: PageType) -> bool:
        """
        Performs some common operation given page type to determine if an event's page
        is of that type.
        """
        pass

    def is_home(self, event: pd.Series) -> bool:
        """
        Checks if page is home page.
        """
        return self.perform_common_operation(event, PageType.HOME)

    def is_about_us(self, event: pd.Series) -> bool:
        """
        Checks if page is a newsletter-form page (e.g., https://19thnews.org/newsletters/daily/).

        A good way to determine if a page is a newsletter page is to consider whether
        making someone subscribe to a newsletter(s) is a primary (or even the only) reason
        that said page exists. Under this criterion, a story or article that has a
        built-in newsletter form shouldn't be considered a newsletter page.
        """
        return self.perform_common_operation(event, PageType.ABOUT_US)

    def is_newsletter(self, event: pd.Series) -> bool:
        """
        Checks if page is an article or story.
        """
        return self.perform_common_operation(event, PageType.NEWSLETTER)

    def is_donation(self, event: pd.Series) -> bool:
        """
        Checks if page is a donation or membership-signup page (e.g., https://dallasfreepress.com/support-dfp/).

        A good way to determine if a page is a donation page is to consider whether
        asking someone to donate is a primary (or even the only) reason
        that said page exists. Under this criterion, a story or article that has a
        built-in donation form shouldn't be considered a donation page.
        """
        return self.perform_common_operation(event, PageType.DONATION)

    def is_article(self, event: pd.Series) -> bool:
        """
        Check if page is a story or article.
        """
        return self.perform_common_operation(event, PageType.ARTICLE)

    def is_section(self, event: pd.Series) -> bool:
        """
        Checks if page is a section, topic, or tag page.
        """
        return self.perform_common_operation(event, PageType.SECTION)

    def is_author_profile(self, event: pd.Series) -> bool:
        """
        Checks if page is author profile and their list of articles.
        """
        return self.perform_common_operation(event, PageType.AUTHOR_PROFILE)


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
        home: str = ANTIPATTERN_URLPATH,
        about_us: str = ANTIPATTERN_URLPATH,
        newsletter: str = ANTIPATTERN_URLPATH,
        donation: str = ANTIPATTERN_URLPATH,
        article: str = ANTIPATTERN_URLPATH,
        section: str = ANTIPATTERN_URLPATH,
        author_profile: str = ANTIPATTERN_URLPATH,
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

    def perform_common_operation(self, event: pd.Series, page_type: PageType) -> bool:
        pattern = cast(re.Pattern[str], getattr(self.patterns, page_type))
        urlpath = event[FieldSnowplow.PAGE_URLPATH]
        return pattern.fullmatch(urlpath) is not None


class SitePageClassifier(PageClassifier, ChangesBetweenTimePeriods):
    """
    Base class of a classifier that consists of one or more classifier
    components.
    """

    def __init__(self, components: List[SitePageClassifierComponent]) -> None:
        self.set_components(components)

    def assign_component(self, event: pd.Series) -> SitePageClassifierComponent:
        return super().assign_component(event[FieldSnowplow.DERIVED_TSTAMP])

    def perform_common_operation(self, event: pd.Series, page_type: PageType) -> bool:
        component = self.assign_component(event)
        page_type_checker = cast(Callable[[pd.Series], bool], getattr(component, f"is_{page_type}"))
        return page_type_checker(event)

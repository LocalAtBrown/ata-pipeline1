import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List

import pandas as pd

from ata_pipeline1.helpers.enums import FieldSnowplow
from ata_pipeline1.helpers.mixins import AppliesFromTimestamp, ChangesBetweenTimestamps


@dataclass
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

    This default component only performs a regex match of an event's `page_urlpath`
    to determine the page type (regex patterns are supplied by the programmer).
    If you need to perform more complex operations to determine page type,
    create a new class that subclasses this class and override methods as needed.
    """

    def __init__(
        self,
        effective_starting: datetime = datetime(1970, 1, 1),
        use_default_patterns_schema: bool = True,
        home: str = " ",
        about_us: str = " ",
        newsletter: str = " ",
        donation: str = " ",
        article: str = " ",
        section: str = " ",
        author_profile: str = " ",
    ) -> None:
        # Using SPACE as default pattern because it always returns False during matching
        # since SPACE isn't allowed in URLs.
        super().__init__(effective_starting=effective_starting)

        if use_default_patterns_schema:
            self.patterns = Patterns(
                home=re.Pattern(home),
                about_us=re.Pattern(about_us),
                newsletter=re.Pattern(newsletter),
                donation=re.Pattern(donation),
                article=re.Pattern(article),
                section=re.Pattern(section),
                author_profile=re.Pattern(author_profile),
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

from abc import ABC

import pandas as pd


class SitePageClassifier(ABC):
    """
    Base class for a site's heuristics-based, multilabel, page-type classifier.
    """

    @staticmethod
    def is_home(event: pd.Series) -> bool:
        """
        Checks if page is home page.
        """
        return False

    @staticmethod
    def is_about_us(event: pd.Series) -> bool:
        """
        Checks if page is an "About us" page, or an "Our team" page, or something
        similar.
        """
        return False

    @staticmethod
    def is_newsletter(event: pd.Series) -> bool:
        """
        Checks if page is a newsletter-form page (e.g., https://19thnews.org/newsletters/daily/).

        A good way to determine if a page is a newsletter page is to consider whether
        making someone subscribe to a newsletter(s) is a primary (or even the only) reason
        that said page exists. Under this criterion, a story or article that has a
        built-in newsletter form shouldn't be considered a newsletter page.
        """
        return False

    @staticmethod
    def is_donation(event: pd.Series) -> bool:
        """
        Checks if page is a donation or membership-signup page (e.g., https://dallasfreepress.com/support-dfp/).

        A good way to determine if a page is a donation page is to consider whether
        asking someone to donate is a primary (or even the only) reason
        that said page exists. Under this criterion, a story or article that has a
        built-in donation form shouldn't be considered a donation page.
        """
        return False

    @staticmethod
    def is_article(event: pd.Series) -> bool:
        """
        Checks if page is an article or story.
        """
        return False

    @staticmethod
    def is_section(event: pd.Series) -> bool:
        """
        Checks if page is a section or topic page.
        """
        return False

    @staticmethod
    def is_author_profile(event: pd.Series) -> bool:
        """
        Checks if page is author profile and eheir list of articles.
        """
        return False


class AfroLaPageClassifier(SitePageClassifier):
    """
    AfroLA page-type classifier.

    TODO: AfroLA's new site design will debut February 1, 2023 at 00:00:00 UTC.
    New logic will need to be written to process all events happening after.
    Some sort of design pattern (and perhaps data structure) that keeps track
    of different sets of rules and when to apply which would also be good
    (maybe a metaclass?).
    """

    pass


class DallasFreePressPageClassifier(SitePageClassifier):
    """
    Dallas Free Press page-type classifier.
    """

    pass


class OpenVallejoPageClassifier(SitePageClassifier):
    """
    Open Vallejo page-type classifier.
    """

    pass


class The19thPageClassifier(SitePageClassifier):
    """
    The 19th* page-type classifier.
    """

    pass

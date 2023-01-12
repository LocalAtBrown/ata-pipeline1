from abc import ABC

import pandas as pd


class SitePageTypeClassifier(ABC):
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

from datetime import datetime
from typing import Any, Callable

import pandas as pd

from ata_pipeline1.helpers.datetime import TIMESTAMP_POSIX
from ata_pipeline1.helpers.enums import PageType
from ata_pipeline1.helpers.re import (
    ANTIPATTERN_URLPATH,
    PATTERN_PAGINATION,
    PATTERN_SLUG,
)
from ata_pipeline1.site.page.base import SitePageClassifier, SitePageClassifierComponent


class DfpBilingualComponent(SitePageClassifierComponent):
    """
    Custom rule set for DFP given that it has pages in both English and Spanish.

    For tests, refer to the sitemap (https://dallasfreepress.com/sitemap_index.xml)
    for test examples of URL paths. In an ideal world, we'd like to build a system that
    monitors this sitemap and changes regex rules accordingly, but hard-coded rules will
    have to work for now.
    """

    def __init__(
        self,
        effective_starting: datetime,
        eng_home: str,
        eng_about_us: str,
        eng_newsletter: str,
        eng_donation: str,
        eng_article: str,
        eng_section: str,
        eng_author_profile: str,
        spa_home: str,
        spa_about_us: str,
        spa_newsletter: str,
        spa_donation: str,
        spa_article: str,
        spa_section: str,
        spa_author_profile: str,
    ) -> None:
        super().__init__(effective_starting=effective_starting, use_default_patterns_schema=False)

        # Separate rules depending on whether language is English or since DFP
        # serves both audiences
        # English
        self.component_eng = SitePageClassifierComponent(
            effective_starting=effective_starting,
            home=eng_home,
            about_us=eng_about_us,
            newsletter=eng_newsletter,
            donation=eng_donation,
            article=eng_article,
            section=eng_section,
            author_profile=eng_author_profile,
        )
        # Spanish
        self.component_spa = SitePageClassifierComponent(
            effective_starting=effective_starting,
            home=spa_home,
            about_us=spa_about_us,
            newsletter=spa_newsletter,
            donation=spa_donation,
            article=spa_article,
            section=spa_section,
            author_profile=spa_author_profile,
        )

    def perform_common_operation(self, event: "pd.Series[Any]", page_type: PageType) -> bool:
        page_type_checker_eng: Callable[["pd.Series[Any]"], bool] = getattr(self.component_eng, f"is_{page_type}")
        page_type_checker_spa: Callable[["pd.Series[Any]"], bool] = getattr(self.component_spa, f"is_{page_type}")
        return page_type_checker_eng(event) or page_type_checker_spa(event)


# Original DFP component
# TODO: Section pages will probably be paginated some day. Keep an eye on it
COMPONENT_ZERO = DfpBilingualComponent(
    effective_starting=TIMESTAMP_POSIX,
    eng_home=rf"^(/{PATTERN_PAGINATION}/?|/)$",
    eng_about_us=r"^/(about\-us|dallas\-free\-press\-editorial\-content|whats\-a\-news\-desert)/?$",
    eng_newsletter=r"^/(text\-and\-email\-notifications|how\-do\-you\-like\-your\-news)/?$",
    eng_donation=r"^/support\-dfp/?$",
    eng_article=rf"^/(dallas\-news|project/{PATTERN_SLUG}?|south\-dallas|uncategorized|west\-dallas)/{PATTERN_SLUG}/?$",
    eng_section=rf"^/(dallas\-forgot|dallas\-news|food\-apartheid|south\-dallas|uncategorized|west\-dallas|tag/{PATTERN_SLUG})/?$",
    eng_author_profile=rf"^/author/{PATTERN_SLUG}/?$",
    spa_home=rf"^/es(/{PATTERN_PAGINATION})?/?$",
    spa_about_us=r"^/es/(sobre\-nosotros|exponiendo\-nuestra\-parcialidad)/?$",
    spa_newsletter=ANTIPATTERN_URLPATH,  # Unlike its English counterpart, the Spanish text-and-email-notifications page https://dallasfreepress.com/es/notificaciones-de-texto-y-correo-electronico/ doesn't have a newsletter form
    spa_donation=r"^/es/apoyanos/?$",
    spa_article=rf"^/es/(noticias\-de\-dallas|south\-dallas|sin\-categorizar|west\-dallas)/{PATTERN_SLUG}/?$",
    spa_section=rf"^/es/(dallas\-forgot|noticias\-de\-dallas|food\-apartheid\-es|south\-dallas|sin\-categorizar|west\-dallas|tag/{PATTERN_SLUG})/?$",
    spa_author_profile=rf"^/es/author/{PATTERN_SLUG}/?$",
)

# When DFP rules change, add a new component here (create a new child class of SitePageClassifierComponent
# as necessary) and add it to the list of components in CLASSIFIER

CLASSIFIER = SitePageClassifier(components=[COMPONENT_ZERO])

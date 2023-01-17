from typing import List

import pandas as pd

from ata_pipeline1.site.page.base import SitePageClassifier, SitePageClassifierComponent


class DallasFreePressComponent(SitePageClassifierComponent):
    pass


class DallasFreePressComponentZero(DallasFreePressComponent):
    """
    Original DFP page-type classifier rules.
    """

    def __init__(
        self,
        en_home=r"^/$",
        en_about_us=r"^/(about|dallas\-free\-press\-editorial\-content|whats\-a\-news\-desert)/?$",
        en_newsletter=r"^/text\-and\-email\-notifications/?$",
        en_donation=r"^/support\-dfp/?$",
        en_article=r"",  # TODO
        en_section=r"^/(dallas\-forgot|dallas\-news)/?$",  # TODO
        en_author_profile=r"^/author/[a-zA-Z\-]+/?$",
        es_home=r"^/es/?$",
        es_about_us=r"^/?$",  # TODO
        es_newsletter=r"^/?$",  # TODO
        es_donation=r"^/es/apoyanos/?$",  # TODO
        es_article=r"^/?$",  # TODO
        es_section=r"^/?$",  # TODO
        es_author_profile=r"^/es/author/[a-zA-Z\-]+/?$",  # TODO
    ) -> None:
        super().__init__(use_default_patterns_schema=False)

        # Separate rules depending on whether language is English or since DFP
        # serves both audiences
        # English
        self.component_en = DallasFreePressComponent(
            home=en_home,
            about_us=en_about_us,
            newsletter=en_newsletter,
            donation=en_donation,
            article=en_article,
            section=en_section,
            author_profile=en_author_profile,
        )
        # Spanish
        self.component_es = DallasFreePressComponent(
            home=es_home,
            about_us=es_about_us,
            newsletter=es_newsletter,
            donation=es_donation,
            article=es_article,
            section=es_section,
            author_profile=es_author_profile,
        )

    def is_home(self, event: pd.Series) -> bool:
        return self.component_en.is_home(event) or self.component_es.is_home(event)


class DallasFreePressPageClassifier(SitePageClassifier):
    """
    Main Dallas Free Press page-type classifier.
    """

    def __init__(self, components: List[DallasFreePressComponent]) -> None:
        super().__init__(components)

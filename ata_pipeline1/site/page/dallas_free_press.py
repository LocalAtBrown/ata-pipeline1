from datetime import datetime

import pandas as pd

from ata_pipeline1.site.page.base import SitePageClassifier, SitePageClassifierComponent


class DfpBilingualComponent(SitePageClassifierComponent):
    """
    Custom rule set for DFP given that it has pages in both English and Spanish.
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
            home=spa_home,
            about_us=spa_about_us,
            newsletter=spa_newsletter,
            donation=spa_donation,
            article=spa_article,
            section=spa_section,
            author_profile=spa_author_profile,
        )

    def is_home(self, event: pd.Series) -> bool:
        return self.component_eng.is_home(event) or self.component_spa.is_home(event)

    def is_about_us(self, event: pd.Series) -> bool:
        return self.component_eng.is_about_us(event) or self.component_spa.is_about_us(event)

    def is_newsletter(self, event: pd.Series) -> bool:
        return self.component_eng.is_newsletter(event) or self.component_spa.is_newsletter(event)

    def is_donation(self, event: pd.Series) -> bool:
        return self.component_eng.is_donation(event) or self.component_spa.is_donation(event)

    def is_article(self, event: pd.Series) -> bool:
        return self.component_eng.is_article(event) or self.component_spa.is_article(event)

    def is_section(self, event: pd.Series) -> bool:
        return self.component_eng.is_section(event) or self.component_spa.is_section(event)

    def is_author_profile(self, event: pd.Series) -> bool:
        return self.component_eng.is_author_profile(event) or self.component_spa.is_author_profile(event)


CLASSIFIER_DALLAS_FREE_PRESS = SitePageClassifier(
    components=[
        DfpBilingualComponent(
            effective_starting=datetime(1970, 1, 1),
            eng_home=r"^/$",
            eng_about_us=r"^/(about|dallas\-free\-press\-editorial\-content|whats\-a\-news\-desert)/?$",
            eng_newsletter=r"^/text\-and\-email\-notifications/?$",
            eng_donation=r"^/support\-dfp/?$",
            eng_article=r"",  # TODO
            eng_section=r"^/(dallas\-forgot|dallas\-news)/?$",  # TODO
            eng_author_profile=r"^/author/[a-zA-Z\-]+/?$",
            spa_home=r"^/es/?$",
            spa_about_us=r"^/?$",  # TODO
            spa_newsletter=r"^/?$",  # TODO
            spa_donation=r"^/es/apoyanos/?$",
            spa_article=r"^/?$",  # TODO
            spa_section=r"^/?$",  # TODO
            spa_author_profile=r"^/es/author/[a-zA-Z\-]+/?$",
        ),
    ]
)

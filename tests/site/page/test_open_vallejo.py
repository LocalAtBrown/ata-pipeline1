import pytest

from ata_pipeline1.helpers.urllib import bulk_append_slash
from ata_pipeline1.site.page.base import Patterns
from ata_pipeline1.site.page.open_vallejo import COMPONENT_ZERO
from tests.site.page.helpers import _test_pattern


@pytest.mark.unit
class TestComponentZero:
    @pytest.fixture(scope="class")
    def patterns(self) -> Patterns:
        return COMPONENT_ZERO.patterns

    def test_home(self, patterns) -> None:
        _test_pattern(patterns.home, bulk_append_slash(["/", "/page/2"]))

    def test_about_us(self, patterns) -> None:
        _test_pattern(
            patterns.about_us,
            bulk_append_slash(
                [
                    "/about-us",
                    "/awards",
                    "/board-and-advisors",
                    "/ethics",
                    "/guest-contributors",
                    "/impact",
                    "/our-team",
                    "/team/geoffrey-king",
                    "/team/leah-chen-price",
                ]
            ),
        )

    def test_newsletter(self, patterns) -> None:
        _test_pattern(patterns.newsletter, bulk_append_slash(["/newsletter"]))

    def test_donation(self, patterns) -> None:
        _test_pattern(patterns.donation, bulk_append_slash(["/donate"]))

    def test_article(self, patterns) -> None:
        # Only test against a representative sample of the articles since it'd be
        # impractical to test against all existing & future articles.
        _test_pattern(
            patterns.article,
            bulk_append_slash(
                [
                    "/2020/12/25/blue-life",
                    "/2022/09/21/vallejo-patrol-staffing-drops-below-1975-levels",
                ]
            ),
        )

    def test_section(self, patterns) -> None:
        # Only test against a representative sample of the sections since it'd be
        # impractical to test against all existing & future sections.
        _test_pattern(
            patterns.section,
            bulk_append_slash(
                [
                    "/category/analysis",
                    "/category/city-hall/city-manager",
                    "/category/news/page/2",
                    "/category/opinion/from-the-editor",
                    "/tag/open-vallejo-news",
                    "/tag/news/page/2",
                ]
            ),
        )

    def test_author_profile(self, patterns) -> None:
        # Only test against a representative sample of the authors since it'd be
        # impractical to test against all existing & future authors.
        _test_pattern(
            patterns.author_profile,
            bulk_append_slash(
                [
                    "/author/laurence-du-sault/",
                    "/author/geoffrey-king/page/2/",
                ]
            ),
        )

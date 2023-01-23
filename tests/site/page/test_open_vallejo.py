import pytest

from ata_pipeline1.site.page.base import Patterns
from ata_pipeline1.site.page.open_vallejo import COMPONENT_ZERO
from tests.site.page.helpers import _test_pattern, append_slash


@pytest.mark.unit
class TestComponentZero:
    @pytest.fixture(scope="class")
    def patterns(self) -> Patterns:
        return COMPONENT_ZERO.patterns

    def test_home(self, patterns) -> None:
        _test_pattern(patterns.home, append_slash(["/", "/page/2"]))

    def test_about_us(self, patterns) -> None:
        _test_pattern(
            patterns.about_us,
            append_slash(
                [
                    "/about-us",
                    "/awards",
                    "/board-and-advisors",
                    "/ethics",
                    "/guest-contributors",
                    "/impact",
                    "/our-team",
                    "/privacy",
                    "/team/geoffrey-king",
                    "/team/leah-chen-price",
                ]
            ),
        )

    def test_newsletter(self, patterns) -> None:
        _test_pattern(patterns.newsletter, append_slash(["/newsletter"]))

    def test_donation(self, patterns) -> None:
        _test_pattern(patterns.donation, append_slash(["/donate"]))

    def test_article(self, patterns) -> None:
        _test_pattern(
            patterns.article,
            append_slash(
                [
                    "/2020/12/25/blue-life",
                    "/2022/09/21/vallejo-patrol-staffing-drops-below-1975-levels",
                ]
            ),
        )

    def test_section(self, patterns) -> None:
        _test_pattern(
            patterns.section,
            append_slash(
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
        _test_pattern(
            patterns.author_profile,
            append_slash(
                [
                    "/author/laurence-du-sault/",
                    "/author/geoffrey-king/page/2/",
                ]
            ),
        )

import pytest

from ata_pipeline1.helpers.url import bulk_append_slash
from ata_pipeline1.site.page.afro_la import COMPONENT_221215, COMPONENT_ZERO
from ata_pipeline1.site.page.base import Patterns
from tests.site.page.helpers import _test_pattern


@pytest.mark.unit
class TestComponentZero:
    @pytest.fixture(scope="class")
    def patterns(self) -> Patterns:
        return COMPONENT_ZERO.patterns

    def test_home(self, patterns) -> None:
        _test_pattern(patterns.home, ["/"])

    def test_about_us(self, patterns) -> None:
        _test_pattern(patterns.about_us, ["/"])

    def test_newsletter(self, patterns) -> None:
        _test_pattern(patterns.newsletter, bulk_append_slash(["/subscribe"]))

    def test_donation(self, patterns) -> None:
        # No dedicated donation page
        pass

    def test_article(self, patterns) -> None:
        _test_pattern(
            patterns.article,
            bulk_append_slash(
                [
                    "/news/black-women-photographers",
                    "/news/eso-won-closing",
                    "/news/roe-reversal",
                    "/news/social-equity",
                    "/news/tec-leimert",
                ]
            ),
        )

    def test_section(self, patterns) -> None:
        _test_pattern(patterns.section, bulk_append_slash(["/news"]))

    def test_author_profile(self, patterns) -> None:
        # Author profile pages doesn't exist yet
        pass


@pytest.mark.unit
class TestComponent221215:
    @pytest.fixture(scope="class")
    def patterns(self) -> Patterns:
        return COMPONENT_221215.patterns

    def test_home(self, patterns) -> None:
        _test_pattern(patterns.home, bulk_append_slash(["/comingsoon"]))

    def test_about_us(self, patterns) -> None:
        _test_pattern(patterns.about_us, bulk_append_slash(["/comingsoon"]))

    def test_newsletter(self, patterns) -> None:
        _test_pattern(patterns.newsletter, bulk_append_slash(["/subscribe"]))

    def test_donation(self, patterns) -> None:
        # No dedicated donation page
        pass

    def test_article(self, patterns) -> None:
        _test_pattern(
            patterns.article,
            bulk_append_slash(
                [
                    "/comingsoon/news/black-women-photographers",
                    "/comingsoon/news/eso-won-closing",
                    "/comingsoon/news/roe-reversal",
                    "/comingsoon/news/social-equity",
                    "/comingsoon/news/tec-leimert",
                ]
            ),
        )

    def test_section(self, patterns) -> None:
        _test_pattern(patterns.section, bulk_append_slash(["/comingsoon/news"]))

    def test_author_profile(self, patterns) -> None:
        # Author profile pages doesn't exist yet
        pass

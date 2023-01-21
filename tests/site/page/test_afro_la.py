import pytest

from ata_pipeline1.site.page.afro_la import COMPONENT_ZERO
from ata_pipeline1.site.page.base import Patterns
from tests.site.page.helpers import _test_pattern, append_slash


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
        _test_pattern(patterns.newsletter, append_slash(["/subscribe"]))

    def test_donation(self, patterns) -> None:
        _test_pattern(patterns.donation, ["/"])

    def test_article(self, patterns) -> None:
        _test_pattern(
            patterns.article,
            append_slash(
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
        _test_pattern(patterns.section, append_slash(["/news"]))

    def test_author_profile(self, patterns) -> None:
        # Author profile pages doesn't exist yet
        pass

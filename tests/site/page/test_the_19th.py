import pytest

from ata_pipeline1.helpers.url import bulk_append_slash
from ata_pipeline1.site.page.base import Patterns
from ata_pipeline1.site.page.the_19th import COMPONENT_ZERO
from tests.site.page.helpers import _test_pattern


@pytest.mark.unit
class TestComponentZero:
    @pytest.fixture(scope="class")
    def patterns(self) -> Patterns:
        return COMPONENT_ZERO.patterns

    def test_home(self, patterns) -> None:
        _test_pattern(patterns.home, bulk_append_slash(["/", "/page/2"]))

    def test_about_us(self, patterns) -> None:
        _test_pattern(patterns.about_us, bulk_append_slash(["/about", "/community-guidelines", "/team"]))

    def test_newsletter(self, patterns) -> None:
        _test_pattern(
            patterns.newsletter,
            bulk_append_slash(["/newsletters", "/newsletters/daily", "/newsletters/events", "/newsletters/weekly"]),
        )

    def test_donation(self, patterns) -> None:
        _test_pattern(patterns.donation, bulk_append_slash(["/membership", "/major-gifts"]))

    def test_article(self, patterns) -> None:
        # Only test against a representative sample of the articles since it'd be
        # impractical to test against all existing & future articles.
        _test_pattern(
            patterns.article,
            bulk_append_slash(
                [
                    "/2020/02/for-klobuchar-and-warren-the-2020-primary-is-an-endurance-race",
                    "/2022/05/election-2022-women-governor-lieutenant-governor",
                    "/2023/01/roe-v-wade-50th-anniversary-abortion-access-changes",
                    "/2023/01/march-for-life-post-roe-anniversary-50-abortion-federal",
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
                    "/archive",
                    "/tag/abortion",
                    "/tag/childrens-health",
                    "/tag/covid-19-vaccine",
                    "/collections/changing-child-care",
                    "/collections/19th-explains",
                    "/topics/election-2022",
                    "/topics/inside-the-19th",
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
                    "/author/abby-johnston",
                    "/author/ann-givens-the-trace-staff-writer",
                    "/author/the-19th-staff",
                ]
            ),
        )

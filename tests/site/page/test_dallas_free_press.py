import pytest

from ata_pipeline1.site.page.base import Patterns
from ata_pipeline1.site.page.dallas_free_press import COMPONENT_ZERO
from tests.site.page.helpers import _test_pattern, append_slash


@pytest.mark.unit
class TestComponentZero:
    @pytest.fixture(scope="class")
    def patterns_eng(self) -> Patterns:
        return COMPONENT_ZERO.component_eng.patterns

    @pytest.fixture(scope="class")
    def patterns_spa(self) -> Patterns:
        return COMPONENT_ZERO.component_spa.patterns

    def test_eng_home(self, patterns_eng) -> None:
        _test_pattern(patterns_eng.home, ["/"])

    def test_spa_home(self, patterns_spa) -> None:
        _test_pattern(patterns_spa.home, append_slash(["/es"]))

    def test_eng_about_us(self, patterns_eng) -> None:
        _test_pattern(
            patterns_eng.about_us,
            append_slash(
                [
                    "/about-us",
                    "/dallas-free-press-editorial-content",
                    "/whats-a-news-desert",
                ]
            ),
        )

    def test_spa_about_us(self, patterns_spa) -> None:
        _test_pattern(patterns_spa.about_us, append_slash(["/es/sobre-nosotros", "/es/exponiendo-nuestra-parcialidad"]))

    def test_eng_newsletter(self, patterns_eng) -> None:
        _test_pattern(
            patterns_eng.newsletter,
            append_slash(
                [
                    "/text-and-email-notifications",
                    "/how-do-you-like-your-news",
                ]
            ),
        )

    def test_spa_newsletter(self, patterns_eng) -> None:
        # Unlike its English counterpart, the Spanish text-and-email-notifications page https://dallasfreepress.com/es/notificaciones-de-texto-y-correo-electronico/
        # doesn't have a newsletter form
        pass

    def test_eng_donation(self, patterns_eng) -> None:
        _test_pattern(
            patterns_eng.donation,
            append_slash(
                [
                    "/support-dfp",
                ]
            ),
        )

    def test_spa_donation(self, patterns_spa) -> None:
        _test_pattern(patterns_spa.donation, append_slash(["/es/apoyanos"]))

    def test_eng_article(self, patterns_eng) -> None:
        _test_pattern(
            patterns_eng.article,
            append_slash(
                [
                    "/dallas-news/what-dallas-isd-parents-are-and-arent-being-told-about-map-tests",
                    "/project/dallas-forgot/freedmans-town-cemetery-community-darrell-school-dallas-history-erasure",
                    "/south-dallas/a-7-million-facelift-will-transform-south-dallas-mlk-school-into-an-arts-academy",
                    "/south-dallas/as-schools-prep-for-virtual-learning-1-3-of-dallas-families-lack-internet-access",
                    "/south-dallas/groceries-in-a-snap-amazon-provides-free-delivery-to-ebt-customers%ef%bf%bc",
                    "/south-dallas/its-2020-but-banks-redlining-practices-still-stifle-southern-dallas",
                    "/uncategorized/south-dallas-residents-deliver-brunch-to-their-unhoused-neighbors",
                    "/west-dallas/does-the-fate-of-west-dallas-rest-on-a-400-foot-tower-next-to-la-bajada",
                    "/west-dallas/pool-amenities-in-west-dallas-fading-entusiasmo-but-changes-are-coming%ef%bf%bc",
                ]
            ),
        )

    def test_spa_article(self, patterns_spa) -> None:
        _test_pattern(
            patterns_spa.article,
            [
                "/es/noticias-de-dallas/la-celebracion-del-16-de-junio-del-centro-comunitario-de-mlk-es-diferente-pero-impactante",
                "/es/south-dallas/adonde-vas-el-sabado-por-la-manana-como-un-plan-de-zonificacion-podria-dar-paso-a-nuevos-negocios-en-el-sur-de-dallas",
                "/es/sin-categorizar/horas-antes-de-la-decision-139-vecinos-de-la-bajada-piden-oposicion-a-la-torre-de-oficinas-de-west-dallas",
                "/es/west-dallas/los-propietarios-de-viviendas-de-west-dallas-no-podian-acceder-a-los-fondos-municipales-para-repararlas-asi-que-el-ayuntamiento-elimino-la-barrera",
            ],
        )

    def test_eng_section(self, patterns_eng) -> None:
        _test_pattern(
            patterns_eng.section,
            append_slash(
                [
                    "/dallas-forgot",
                    "/dallas-news",
                    "/food-apartheid",
                    "/south-dallas",
                    "/west-dallas",
                    "/uncategorized",
                    "/tag/community-meeting",
                    "/tag/2021",
                    "/tag/7-eleven",
                ]
            ),
        )

    def test_spa_section(self, patterns_spa) -> None:
        _test_pattern(
            patterns_spa.section,
            append_slash(
                [
                    "/es/dallas-forgot",
                    "/es/noticias-de-dallas",
                    "/es/food-apartheid-es",
                    "/es/south-dallas",
                    "/es/sin-categorizar",
                    "/es/west-dallas",
                    "/es/tag/community-meeting",
                    "/es/tag/2021",
                    "/es/tag/7-eleven",
                ]
            ),
        )

    def test_eng_author_profile(self, patterns_eng) -> None:
        _test_pattern(
            patterns_eng.author_profile,
            append_slash(
                [
                    "/author/fatima-syed",
                    "/author/christine-hughes-babb",
                    "/author/keri",
                ]
            ),
        )

    def test_spa_author_profile(self, patterns_spa) -> None:
        _test_pattern(
            patterns_spa.author_profile,
            append_slash(
                [
                    "/es/author/fatima-syed",
                    "/es/author/christina-hughes-babb",
                    "/es/author/keri",
                ]
            ),
        )

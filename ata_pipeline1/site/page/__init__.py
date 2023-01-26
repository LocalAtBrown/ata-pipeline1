__all__ = [
    "SitePageClassifier",
    "SitePageClassifierComponent",
    "PAGE_CLASSIFIER_AFRO_LA",
    "PAGE_CLASSIFIER_DALLAS_FREE_PRESS",
    "PAGE_CLASSIFIER_OPEN_VALLEJO",
    "PAGE_CLASSIFIER_THE_19TH",
]

from ata_pipeline1.site.page.afro_la import CLASSIFIER as PAGE_CLASSIFIER_AFRO_LA
from ata_pipeline1.site.page.base import SitePageClassifier, SitePageClassifierComponent
from ata_pipeline1.site.page.dallas_free_press import (
    CLASSIFIER as PAGE_CLASSIFIER_DALLAS_FREE_PRESS,
)
from ata_pipeline1.site.page.open_vallejo import (
    CLASSIFIER as PAGE_CLASSIFIER_OPEN_VALLEJO,
)
from ata_pipeline1.site.page.the_19th import CLASSIFIER as PAGE_CLASSIFIER_THE_19TH

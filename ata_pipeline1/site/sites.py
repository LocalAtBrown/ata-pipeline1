from dataclasses import dataclass

from ata_pipeline1.site.names import SiteName
from ata_pipeline1.site.newsletter import (
    AfroLaNewsletterSignupValidator,
    DallasFreePressNewsletterSignupValidator,
    OpenVallejoNewsletterSignupValidator,
    SiteNewsletterSignupValidator,
    The19thNewsletterSignupValidator,
)
from ata_pipeline1.site.page import (
    PAGE_CLASSIFIER_AFRO_LA,
    PAGE_CLASSIFIER_DALLAS_FREE_PRESS,
    PAGE_CLASSIFIER_OPEN_VALLEJO,
    PAGE_CLASSIFIER_THE_19TH,
    SitePageClassifier,
)


@dataclass
class Site:
    """
    Base site class.
    """

    name: SiteName
    newsletter_signup_validator: SiteNewsletterSignupValidator
    page_type_classifier: SitePageClassifier


# ---------- SITE OBJECTS ----------
AFRO_LA = Site(
    name=SiteName.AFRO_LA,
    newsletter_signup_validator=AfroLaNewsletterSignupValidator(),
    page_type_classifier=PAGE_CLASSIFIER_AFRO_LA,
)
DALLAS_FREE_PRESS = Site(
    name=SiteName.DALLAS_FREE_PRESS,
    newsletter_signup_validator=DallasFreePressNewsletterSignupValidator(),
    page_type_classifier=PAGE_CLASSIFIER_DALLAS_FREE_PRESS,
)
OPEN_VALLEJO = Site(
    name=SiteName.OPEN_VALLEJO,
    newsletter_signup_validator=OpenVallejoNewsletterSignupValidator(),
    page_type_classifier=PAGE_CLASSIFIER_OPEN_VALLEJO,
)
THE_19TH = Site(
    name=SiteName.THE_19TH,
    newsletter_signup_validator=The19thNewsletterSignupValidator(),
    page_type_classifier=PAGE_CLASSIFIER_THE_19TH,
)

SITES = {
    SiteName.AFRO_LA: AFRO_LA,
    SiteName.DALLAS_FREE_PRESS: DALLAS_FREE_PRESS,
    SiteName.OPEN_VALLEJO: OPEN_VALLEJO,
    SiteName.THE_19TH: THE_19TH,
}

from dataclasses import dataclass

from ata_pipeline1.site.names import SiteName
from ata_pipeline1.site.newsletter import (
    NEWSLETTER_SIGNUP_VALIDATOR_AFRO_LA,
    NEWSLETTER_SIGNUP_VALIDATOR_DALLAS_FREE_PRESS,
    NEWSLETTER_SIGNUP_VALIDATOR_OPEN_VALLEJO,
    NEWSLETTER_SIGNUP_VALIDATOR_THE_19TH,
    SiteNewsletterSignupValidator,
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
    newsletter_signup_validator=NEWSLETTER_SIGNUP_VALIDATOR_AFRO_LA,
    page_type_classifier=PAGE_CLASSIFIER_AFRO_LA,
)
DALLAS_FREE_PRESS = Site(
    name=SiteName.DALLAS_FREE_PRESS,
    newsletter_signup_validator=NEWSLETTER_SIGNUP_VALIDATOR_DALLAS_FREE_PRESS,
    page_type_classifier=PAGE_CLASSIFIER_DALLAS_FREE_PRESS,
)
OPEN_VALLEJO = Site(
    name=SiteName.OPEN_VALLEJO,
    newsletter_signup_validator=NEWSLETTER_SIGNUP_VALIDATOR_OPEN_VALLEJO,
    page_type_classifier=PAGE_CLASSIFIER_OPEN_VALLEJO,
)
THE_19TH = Site(
    name=SiteName.THE_19TH,
    newsletter_signup_validator=NEWSLETTER_SIGNUP_VALIDATOR_THE_19TH,
    page_type_classifier=PAGE_CLASSIFIER_THE_19TH,
)

SITES = {
    SiteName.AFRO_LA: AFRO_LA,
    SiteName.DALLAS_FREE_PRESS: DALLAS_FREE_PRESS,
    SiteName.OPEN_VALLEJO: OPEN_VALLEJO,
    SiteName.THE_19TH: THE_19TH,
}

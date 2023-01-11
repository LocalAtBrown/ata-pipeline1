from abc import ABC
from dataclasses import dataclass
from typing import Dict

from ata_pipeline1.site.names import SiteName
from ata_pipeline1.site.newsletter import (
    AfroLaNewsletterSignupValidator,
    DallasFreePressNewsletterSignupValidator,
    OpenVallejoNewsletterSignupValidator,
    SiteNewsletterSignupValidator,
    The19thNewsletterSignupValidator,
)


# ---------- SITE CLASSES ----------
@dataclass
class Site(ABC):
    """
    Base site class.
    """

    name: SiteName
    newsletter_signup_validator: SiteNewsletterSignupValidator


@dataclass
class AfroLa(Site):
    """
    AfroLA site class.
    """

    name: SiteName = SiteName.AFRO_LA
    newsletter_signup_validator: SiteNewsletterSignupValidator = AfroLaNewsletterSignupValidator()


@dataclass
class DallasFreePress(Site):
    """
    Dallas Free Press site class.
    """

    name: SiteName = SiteName.DALLAS_FREE_PRESS
    newsletter_signup_validator: SiteNewsletterSignupValidator = DallasFreePressNewsletterSignupValidator()


@dataclass
class OpenVallejo(Site):
    """
    OpenVallejo site class.
    """

    name: SiteName = SiteName.OPEN_VALLEJO
    newsletter_signup_validator: SiteNewsletterSignupValidator = OpenVallejoNewsletterSignupValidator()


@dataclass
class The19th(Site):
    """
    The 19th site class.
    """

    name: SiteName = SiteName.THE_19TH
    newsletter_signup_validator: SiteNewsletterSignupValidator = The19thNewsletterSignupValidator()


# Site objects
SITES: Dict[SiteName, Site] = {
    SiteName.AFRO_LA: AfroLa(),
    SiteName.DALLAS_FREE_PRESS: DallasFreePress(),
    SiteName.OPEN_VALLEJO: OpenVallejo(),
    SiteName.THE_19TH: The19th(),
}

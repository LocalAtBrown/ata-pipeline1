from datetime import datetime

from ata_pipeline1.helpers.datetime import TIMESTAMP_POSIX
from ata_pipeline1.helpers.re import ANTIPATTERN_URLPATH, PATTERN_SLUG
from ata_pipeline1.site.page.base import SitePageClassifier, SitePageClassifierComponent

# Original AfroLA component
COMPONENT_ZERO = SitePageClassifierComponent(
    effective_starting=TIMESTAMP_POSIX,
    home=r"^/$",
    about_us=r"^/$",  # Same as home
    newsletter=r"^/subscribe/?$",
    donation=ANTIPATTERN_URLPATH,  # No dedicated donation page
    article=rf"^/news/{PATTERN_SLUG}/?$",
    section=r"^/news/?$",
    author_profile=ANTIPATTERN_URLPATH,  # No author page
)

# Pre-launch period
COMPONENT_221215 = SitePageClassifierComponent(
    effective_starting=datetime(
        2022, 12, 15, 12, 0, 0
    ),  # the hour-min-sec mark was determined by looking at the database
    home=r"^/comingsoon/?$",
    about_us=r"^/comingsoon/?$",  # Same as home
    newsletter=r"^/subscribe/?$",
    donation=ANTIPATTERN_URLPATH,  # No dedicated donation page
    article=rf"^/comingsoon/news/{PATTERN_SLUG}/?$",
    section=r"^/comingsoon/news/?$",
    author_profile=ANTIPATTERN_URLPATH,  # No author page
)

# Post-launch period
COMPONENT_230201 = SitePageClassifierComponent(effective_starting=datetime(2023, 2, 1))

# When AfroLA rules change, add a new component here (create a new child class of SitePageClassifierComponent
# as necessary) and add it to the list of components in CLASSIFIER

CLASSIFIER = SitePageClassifier(components=[COMPONENT_ZERO, COMPONENT_221215])

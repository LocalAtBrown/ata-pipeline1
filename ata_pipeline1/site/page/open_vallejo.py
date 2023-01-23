from ata_pipeline1.helpers.datetime import TIMESTAMP_POSIX
from ata_pipeline1.helpers.re import PATTERN_DATE, PATTERN_PAGINATION, PATTERN_SLUG
from ata_pipeline1.site.page.base import SitePageClassifier, SitePageClassifierComponent

# Original OpenVallejo component
# Refer to OpenVallejo's site map: https://openvallejo.org/sitemap_index.xml
COMPONENT_ZERO = SitePageClassifierComponent(
    effective_starting=TIMESTAMP_POSIX,
    home=rf"^(/{PATTERN_PAGINATION}/?|/)$",
    about_us=rf"^/(about\-us|awards|our\-team|guest\-contributors|ethics|privacy|board\-and\-advisors|impact|team/{PATTERN_SLUG})/?$",
    newsletter=r"^/newsletter/?$",
    donation=r"^/donate/?$",
    article=rf"^/{PATTERN_DATE}/{PATTERN_SLUG}/?$",
    section=rf"^/(category/({PATTERN_SLUG}/)*?{PATTERN_SLUG}|tag/{PATTERN_SLUG})(/{PATTERN_PAGINATION})?/?$",
    author_profile=rf"^/author/{PATTERN_SLUG}(/{PATTERN_PAGINATION})?/?$",
)

# When OpenVallejo rules change, add a new component here (create a new child class of SitePageClassifierComponent
# as necessary) and add it to the list of components in CLASSIFIER

CLASSIFIER = SitePageClassifier([COMPONENT_ZERO])

from ata_pipeline1.helpers.datetime import TIMESTAMP_POSIX
from ata_pipeline1.helpers.re import PATTERN_DATE_YM, PATTERN_PAGINATION, PATTERN_SLUG
from ata_pipeline1.site.page.base import SitePageClassifier, SitePageClassifierComponent

# Original The 19th* component
# Refer to sitemap: https://19thnews.org/sitemap_index.xml
COMPONENT_ZERO = SitePageClassifierComponent(
    effective_starting=TIMESTAMP_POSIX,
    home=rf"^(/{PATTERN_PAGINATION}/?|/)$",
    about_us=r"^/(about|community\-guidelines|team)/?$",
    newsletter=rf"^/newsletters(/{PATTERN_SLUG})*/?$",
    donation=r"^/(membership|major\-gifts)/?$",
    article=rf"^/{PATTERN_DATE_YM}/{PATTERN_SLUG}/?$",  # not including special projects & interactives, e.g., https://19thnews.org/arizona-6th-congressional-district-2020-results-hiral-tipirneni-david-schweikert/
    section=rf"^/(archive|(collections|tag|topics)/{PATTERN_SLUG})(/{PATTERN_PAGINATION})?/?",
    author_profile=rf"^/author/{PATTERN_SLUG}(/{PATTERN_PAGINATION})?/?$",
)

# When The 19th* rules change, add a new component here (create a new child class of SitePageClassifierComponent
# as necessary) and add it to the list of components in CLASSIFIER

CLASSIFIER = SitePageClassifier(components=[COMPONENT_ZERO])

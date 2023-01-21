# Matches an article, author or tag/section slug
PATTERN_SLUG = r"[a-zA-Z\d\-%]+"

# Matches a pagination page with its page number
PATTERN_PAGINATION = r"page/\d+"

# URL-path antipattern string that will definitely never match anything inside an
# actual URL
ANTIPATTERN_URLPATH = r"\s<>%{}`"

# URL-path antipattern string that will definitely never match anything inside an
# actual URL
ANTIPATTERN_URLPATH = r"\s<>%{}`"

# Matches a pagination page with its page number
PATTERN_PAGINATION = r"page/\d+"

# Matches an article, author or tag/section slug
PATTERN_SLUG = r"[a-zA-Z\d\-%]+"

# Matches a yyyy/mm date
PATTERN_DATE_YM = r"20\d{2}/(0[1-9]|1[012])"

# Matches a yyyy/mm/dd date
PATTERN_DATE_YMD = r"20\d{2}/(0[1-9]|1[012])/(0[1-9]|[12][0-9]|3[01])"

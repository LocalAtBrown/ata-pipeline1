from ata_pipeline1.preprocessors.aggregate import AggregatePageActivities
from ata_pipeline1.preprocessors.non_aggregate import (
    AddFieldDeviceIsMobile,
    AddFieldEventParentId,
    AddFieldFormSubmitIsNewsletter,
    AddFieldLeadsToNewsletterConversion,
    AddFieldMaxScrollDepth,
    AddFieldSessionEventIndex,
    AddFieldsPageType,
    ClipRowsOutlier,
    ConvertFieldTypes,
    DeleteRowsOutlier,
    ParseInternalReferrerUrls,
    ReclassifyInternalReferrals,
    ReclassifyNullReferrals,
    ReplaceValues,
    SetRowIndex,
    SortFieldTimestamp,
)

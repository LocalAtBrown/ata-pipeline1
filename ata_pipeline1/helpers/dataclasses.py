from dataclasses import dataclass
from typing import Any, Dict, List, Optional


# ---------- FORM SUBMISSION DATA ----------
@dataclass(frozen=True)
class FormElement:
    """
    JSON data schema of an element as part of Snowplow form-submission event data
    (see: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    """

    # These constraints are looser than what the schema requires. If we need
    # them to be stricter, consider pydantic.
    name: str
    node_name: str
    value: Optional[str] = None
    type: Optional[str] = None


@dataclass(frozen=True)
class FormSubmitData:
    """
    JSON data schema of a Snowplow form-submission event
    (see: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0)
    """

    # These constraints are looser than what the schema requires. If we need
    # them to be stricter, consider pydantic.
    form_id: str
    form_classes: List[str]
    elements: List[FormElement]


def parse_form_submit_dict(data: Dict[str, Any]) -> FormSubmitData:
    """
    Creates a dataclass from a corresponding `dict` of form-submission data.
    """
    return FormSubmitData(
        form_id=data["formId"],
        form_classes=data["formClasses"],
        elements=[
            FormElement(name=e["name"], node_name=e["nodeName"], value=e.get("value"), type=e.get("type"))
            for e in data["elements"]
        ],
    )

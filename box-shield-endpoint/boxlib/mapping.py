from typing import Any, Dict, Optional


def map_dxr_label_to_classification(mapping: Dict[str, str], label: Dict[str, Any]) -> Optional[str]:
    """
    Very basic mapping: prefer subtype then type then default.
    Expected DXR label shape: { 'type': 'pii', 'subtype': 'ssn', ... }
    mapping examples:
      { 'default': 'Internal', 'pii': 'Confidential', 'phi': 'Highly Confidential' }
    """
    subtype = str(label.get("subtype", "")).lower()
    ltype = str(label.get("type", "")).lower()
    if subtype and subtype in mapping:
        return mapping[subtype]
    if ltype and ltype in mapping:
        return mapping[ltype]
    return mapping.get("default")


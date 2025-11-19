POLICY_SCHEMA = {
    "type": "object",
    "required": ["allow", "reasons", "flags"],
    "properties": {
        "allow": {"type": "boolean"},
        "reasons": {"type": "array", "items": {"type": "string"}},
        "flags": {"type": "array", "items": {"type": "string"}},
    },
}

WEEKLY_DEEP_DIVE_SCHEMA = {
    "type": "object",
    "required": ["risk_flag", "sentiment", "comment", "catalysts", "implications", "stance", "agreement"],
    "properties": {
        "risk_flag": {"type": "string", "enum": ["normal", "elevated", "avoid"]},
        "sentiment": {"type": "number"},
        "comment": {"type": "string"},
        "catalysts": {"type": "array", "items": {"type": "string"}},
        "implications": {"type": "string"},
        "stance": {"type": "string", "enum": ["bullish", "bearish", "neutral"]},
        "agreement": {"type": "integer", "enum": [-1, 0, 1]},
    },
}

DAILY_GO_NOGO_SCHEMA = {
    "type": "object",
    "required": ["go", "confidence", "reasons", "recommended_action", "triggers"],
    "properties": {
        "go": {"type": "boolean"},
        "confidence": {"type": "number"},
        "reasons": {"type": "array", "items": {"type": "string"}},
        "recommended_action": {"type": "string"},
        "triggers": {"type": "array", "items": {"type": "string"}},
    },
}

WEEKLY_GO_NOGO_SCHEMA = {
    "type": "object",
    "required": ["decision", "confidence", "rationale", "signals"],
    "properties": {
        "decision": {"type": "string", "enum": ["go", "no_go", "neutral"]},
        "confidence": {"type": "number"},
        "rationale": {"type": "string"},
        "signals": {"type": "array", "items": {"type": "string"}},
        "alerts": {"type": "array", "items": {"type": "string"}},
        "alignment": {"type": "string", "enum": ["agree", "disagree", "mixed"]},
    },
}

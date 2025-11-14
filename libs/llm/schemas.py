POLICY_SCHEMA = {
    "type": "object",
    "required": ["allow", "reasons", "flags"],
    "properties": {
        "allow": {"type": "boolean"},
        "reasons": {"type": "array", "items": {"type": "string"}},
        "flags": {"type": "array", "items": {"type": "string"}},
    },
}

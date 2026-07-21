"""R15: Indian PIN code normalizer for chaotic last-mile addresses.

Real-world address lines mangle PINs freely: "400 001", "400001.", "PIN-400001",
"Mumbai - 400 001", letter/digit confusions ("4OO0O1"). normalize_pincode
recovers a valid 6-digit PIN or returns None (never guesses a plausible-looking
but invalid code).

Valid Indian PINs: 6 digits, first digit 1-8 (0 and 9 are not assigned zones).
"""

import re

# Common OCR/typing confusions seen in address feeds.
_CONFUSABLES = str.maketrans({"O": "0", "o": "0", "I": "1", "l": "1", "S": "5", "B": "8"})

_PIN_RE = re.compile(r"(?<!\d)([1-8]\d{2})\s*[- ]?\s*(\d{3})(?!\d)")


def normalize_pincode(raw: str) -> str | None:
    """Extract and normalize a PIN code from an address fragment."""
    if not raw:
        return None
    cleaned = raw.translate(_CONFUSABLES)
    matches = _PIN_RE.findall(cleaned)
    if not matches:
        return None
    # If several 6-digit groups appear, the PIN is conventionally the last one
    # (addresses end with the PIN).
    a, b = matches[-1]
    return a + b

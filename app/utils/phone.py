import re

BR_DIAL_CODE = "55"

digits_re = re.compile(r"\D+")
phone_re = re.compile(r"(?:\+?55)?\s*\(?\d{2}\)?\s*9?\d{4}\-?\d{4}")

def extract_phones_from_text(text: str) -> list[str]:
    phones = set()
    for m in phone_re.finditer(text or ""):
        raw = m.group(0)
        norm = normalize_br(raw)
        if norm:
            phones.add(norm)
    return list(phones)

def normalize_br(s: str) -> str | None:
    if not s:
        return None
    d = digits_re.sub("", s)
    if d.startswith("00"):
        d = d[2:]
    if d.startswith(BR_DIAL_CODE):
        d = d[len(BR_DIAL_CODE):]
    # remove leading zeros
    d = d.lstrip("0")
    # Expect 10 or 11 digits after DDD for BR
    if len(d) == 10 or len(d) == 11:
        return f"+{BR_DIAL_CODE}{d}"
    # Sometimes numbers appear without DDD. Reject.
    return None

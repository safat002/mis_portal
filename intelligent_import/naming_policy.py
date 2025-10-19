import re
import unicodedata

# Reserved words list (Postgres short set)
RESERVED = {"user","order","group","select","where","table","column","count","limit","offset"}

def normalize_snake(s: str, maxlen=60):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    s = re.sub(r"^[0-9_]+", "", s)           # strip leading digits/underscores
    if not s:
        s = "x"
    if s in RESERVED:
        s = f"{s}_col"
    return s[:maxlen].rstrip("_")

def table_name(role: str, label: str):
    role = "fact" if (role or "").lower() not in {"fact","ref"} else role.lower()
    topic = normalize_snake(label, maxlen=57)
    base = f"{role}_{topic}" if topic else f"{role}_x"
    return base[:63]  # PG limit

def resolve_template_table_name(value: str) -> str:
    """Resolve a template-provided table value into a safe SQL identifier.

    - Strips special UI prefixes like "__new__:" or "__reuse_new__:".
    - Normalizes to snake_case and enforces length/character rules.
    """
    v = (value or "").strip()
    # Strip various template builder prefixes like new/newcol/newtable with or without underscores
    v = re.sub(r"^(?:__?reuse_new__?:|__?new(?:col|table)?__?:|new(?:col|table)?:)\s*", "", v, flags=re.IGNORECASE)
    return normalize_snake(v, maxlen=63)

def resolve_template_column_name(value: str) -> str:
    """Resolve a template-provided column value into a safe SQL identifier.

    - Strips special UI prefixes like "__newcol__:", "__new__:", "__reuse_new__:".
    - Normalizes to snake_case and enforces length/character rules.
    """
    v = (value or "").strip()
    # Strip various template builder prefixes like new/newcol/newtable with or without underscores
    v = re.sub(r"^(?:__?reuse_new__?:|__?new(?:col|table)?__?:|new(?:col|table)?:)\s*", "", v, flags=re.IGNORECASE)
    return normalize_snake(v, maxlen=63)

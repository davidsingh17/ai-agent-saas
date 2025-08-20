import re
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

# ============================== NUMERI ==============================
# Supporta: 1.234,56 | 1234,56 | 1234.56 | 14,00
NUM_RE = r"(?:(?:\d{1,3}[\.,])*(?:\d{1,3})(?:[\.,]\d{2})?)"

def _to_float_eu(x: str) -> Optional[float]:
    if not x:
        return None
    x = x.strip().replace(" ", "")
    # 1.234,56 -> 1234.56  |  1234,56 -> 1234.56  |  1234.56 -> 1234.56
    if x.count(",") == 1:
        x = x.replace(".", "").replace(",", ".")
    try:
        return float(x)
    except Exception:
        return None


# ============================== DATE ==============================
MONTHS_IT = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5, "giugno": 6,
    "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
    # abbreviazioni comuni
    "gen": 1, "feb": 2, "mar": 3, "apr": 4, "mag": 5, "giu": 6,
    "lug": 7, "ago": 8, "set": 9, "ott": 10, "nov": 11, "dic": 12,
}
PLACEHOLDER_TOKENS = re.compile(r"(?:\b|_)(y{2,4}|m{2}|d{2})(?:\b|_)", re.I)

def _safe_date(y: int, m: int, d: int) -> Optional[str]:
    try:
        return datetime(y, m, d).date().isoformat()
    except Exception:
        return None

def _find_date(text: str) -> Optional[str]:
    t = text.lower()

    # 1) dd/mm/yyyy | dd-mm-yyyy | dd.mm.yyyy
    m = re.search(r"\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})\b", t)
    if m and not PLACEHOLDER_TOKENS.search(m.group(0)):
        d, mo, y = map(int, m.groups())
        return _safe_date(y, mo, d)

    # 2) dd/mm/yy (pivot 50)
    m = re.search(r"\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2})\b", t)
    if m and not PLACEHOLDER_TOKENS.search(m.group(0)):
        d, mo, yy = map(int, m.groups())
        y = 2000 + yy if yy < 50 else 1900 + yy
        return _safe_date(y, mo, d)

    # 3) yyyy-mm-dd
    m = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", t)
    if m and not PLACEHOLDER_TOKENS.search(m.group(0)):
        y, mo, d = map(int, m.groups())
        return _safe_date(y, mo, d)

    # 4) 14 ottobre 2025 (accetta abbreviazioni tipo "ott.")
    m = re.search(r"\b(\d{1,2})\s+([a-zàèéìòù\.]{3,10})\s+(\d{4})\b", t, re.IGNORECASE)
    if m:
        d_s, mon_raw, y_s = m.groups()
        mon = mon_raw.replace(".", "")
        if mon in MONTHS_IT:
            return _safe_date(int(y_s), MONTHS_IT[mon], int(d_s))

    return None


# ============================== FORNITORE ==============================
# Blacklist aggressiva: evita intestazioni, etichette, recapiti, campi fiscali.
SUPPLIER_BLACKLIST = re.compile(
    r"\b("
    r"fattura|preventivo|invoice|"
    r"numero\s*fattura|n[°ºo]\s*fattura|data\s*fattura|"
    r"descrizione|quantit|q\.?t[àa]|prezzo|subtotale|imponibile|totale|iva|i\.?v\.?a\.?|"
    r"partita\s*iva|p\.?\s*iva|cod(?:\.|ice)?\s*fisc|c\.?\s*f\.|codice\s*fiscale|"
    r"iban|bic|swift|pec|e-?mail|email|telefono|tel\.|cell\.|fax|"
    r"indirizzo|via|viale|piazza|corso|largo|cap|citt[aà']|prov\.?"
    r")\b",
    re.IGNORECASE,
)
ADDRESS_RE = re.compile(r"\b(via|viale|piazza|corso|largo)\b.*\d", re.IGNORECASE)
ONLY_NUMERICISH = re.compile(r"^[\W_]*\d[\d\W_]*$")
LEGAL_FORM = re.compile(r"\b(srl|s\.r\.l\.|spa|s\.p\.a\.|sas|snc|ss|coop|cooperativa)\b", re.IGNORECASE)
CLEAN_PREFIX = re.compile(r"^[\s:\-\[\(\{«]+")
CLEAN_SUFFIX = re.compile(r"[\]\)\}»\s:;\-]+$")

def _clean_name(s: str) -> str:
    s = CLEAN_PREFIX.sub("", s or "").strip()
    s = CLEAN_SUFFIX.sub("", s).strip()
    return s

def _guess_supplier(text: str) -> Optional[str]:
    lines = [ln for ln in (ln.strip() for ln in text.splitlines()) if ln][:25]
    candidates = []
    for ln in lines:
        ln_norm = _clean_name(ln)
        if SUPPLIER_BLACKLIST.search(ln_norm):
            continue
        if ADDRESS_RE.search(ln_norm):
            continue
        if ONLY_NUMERICISH.match(ln_norm):
            continue
        candidates.append(ln_norm)

    if not candidates:
        return None

    # 1) priorità a chi ha forma giuridica
    for ln in candidates:
        if LEGAL_FORM.search(ln):
            return ln

    # 2) molte lettere e poche cifre
    for ln in candidates:
        letters = sum(ch.isalpha() for ch in ln)
        digits  = sum(ch.isdigit() for ch in ln)
        if letters >= 6 and digits <= 2:
            return ln

    # 3) fallback
    return candidates[0]


# ============================== EXTRA CAMPI ==============================
VAT_RE      = re.compile(r"\b(?:IT)?\s*([0-9]{11})\b", re.IGNORECASE)  # P.IVA 11 cifre (con IT opzionale)
CF_RE       = re.compile(r"\b([A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{3}[A-Z])\b", re.IGNORECASE)  # CF 16 car.
ANY_11_DIG  = re.compile(r"\b\d{11}\b")  # fallback 11 cifre
VAT_LABEL   = re.compile(r"\b(?:p\.?\s*iva|partita\s*iva)\b[:\s]*([A-Z0-9\.\s/-]{8,})", re.I)
CF_LABEL    = re.compile(r"\b(?:cod(?:ice)?\s*fisc(?:ale)?)\b[:\s]*([A-Z0-9]{11,20})", re.I)
CF_PATTERN  = re.compile(r"^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$", re.I)

ACC_HOLDER_RE = re.compile(
    r"\b(intestatario|intestato\s*a|beneficiario|titolare(?:\s+conto)?)\b[:\s-]*(.+)",
    re.I
)

# Numero fattura
NUM_TOKEN_RE = re.compile(r"\b((?:inv|invoice|fatt|fa)[-_][A-Z0-9][A-Z0-9/_\-]*)\b", re.I)
INVOICE_NUM_RES = [
    r"\bfattura\s*(?:n|nr|no|n[°º]|numero)?\s*[:#]?\s*([A-Z0-9/_\-.]+)",
    r"\b(?:invoice|inv)\s*(?:n|#)?\s*[:#]?\s*([A-Z0-9/_\-.]+)",
    r"\b(?:documento|doc)\s*(?:n|nr|no|numero)?\s*[:#]?\s*([A-Z0-9/_\-.]+)",
]

def _clean_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def _find_tax_ids(text: str) -> Tuple[Optional[str], Optional[str]]:
    vat = None
    cf  = None

    m = VAT_LABEL.search(text)
    if m:
        vat = _clean_digits(m.group(1))
        if len(vat) != 11:
            vat = None
    if not vat:
        m = ANY_11_DIG.search(text)
        if m:
            vat = m.group(0)

    m = CF_LABEL.search(text)
    if m:
        cand = m.group(1).replace(" ", "").upper()
        if CF_PATTERN.match(cand):
            cf = cand
    return vat, cf

def _find_account_holder(text: str) -> Optional[str]:
    for ln in text.splitlines():
        m = ACC_HOLDER_RE.search(ln)
        if m:
            name = _clean_name(m.group(2))
            if len(name) >= 3:
                return name
    return None

def _looks_like_date(s: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", s))

def _find_invoice_number(text: str) -> Optional[str]:
    # 1) token stile inv-2025-001
    m = NUM_TOKEN_RE.search(text)
    if m:
        num = m.group(1).strip()
        if not _looks_like_date(num):
            return num

    # 2) etichette classiche
    t = text.lower()
    for pat in INVOICE_NUM_RES:
        m = re.search(pat, t, re.I)
        if m:
            num = m.group(1).strip().strip(":#")
            if not _looks_like_date(num) and len(num) >= 3:
                return num
    return None


# ============================== LINE HELPERS ==============================
def _find_amount_on_line(label: str, text: str, forbid_percent: bool = True) -> Optional[float]:
    """Cerca il primo importo sulla *stessa riga* che contiene 'label'."""
    lab = label.lower()
    for ln in text.splitlines():
        l = ln.lower()
        if lab in l:
            if forbid_percent and "%" in l:
                continue
            m = re.search(NUM_RE, ln)
            if m:
                return _to_float_eu(m.group(0))
    return None

def _find_vat_percent(text: str) -> Optional[float]:
    # "IVA 22%" / "I.V.A. 10 %"
    for ln in text.splitlines():
        l = ln.lower()
        if "iva" in l and "%" in l:
            m = re.search(r"(\d{1,2})(?:,\d+)?\s*%", l)
            if m:
                try:
                    return float(m.group(1).replace(",", ".")) / 100.0
                except Exception:
                    return None
    return None


# ============================== PARSER FATTURA ==============================
def parse_invoice(text: str) -> Dict[str, Any]:
    supplier        = _guess_supplier(text)
    account_holder  = _find_account_holder(text)
    vat_id, tax_code = _find_tax_ids(text)
    number          = _find_invoice_number(text)
    date            = _find_date(text)

    # importi (line-aware)
    net = (
        _find_amount_on_line("imponibile", text)
        or _find_amount_on_line("subtotale", text)
        or _find_amount_on_line("imponib.", text)
    )
    tot = (
        _find_amount_on_line("totale", text, forbid_percent=False)
        or _find_amount_on_line("totale da pagare", text, forbid_percent=False)
        or _find_amount_on_line("importo totale", text, forbid_percent=False)
        or _find_amount_on_line("da pagare", text, forbid_percent=False)
    )
    vat = _find_amount_on_line("iva", text)
    vat_pct = _find_vat_percent(text)

    # ricostruzioni / coerenza
    if vat is None and vat_pct is not None and net is not None:
        vat = round(net * vat_pct, 2)
    if tot is None and net is not None and vat is not None:
        tot = round(net + vat, 2)
    if vat is None and tot is not None and net is not None:
        diff = round(tot - net, 2)
        if diff >= 0:
            vat = diff

    # falsi positivi IVA
    if vat is not None and tot is not None:
        if vat >= tot - 0.01:
            vat = None
        elif vat > (tot * 0.6):
            vat = None

    # solo totale + %: ricava net/iva
    if net is None and tot is not None and vat_pct is not None:
        base = round(tot / (1.0 + vat_pct), 2)
        net = base
        vat = round(tot - base, 2)

    currency = "EUR" if ("€" in text or " eur" in text.lower()) else None

    found = sum(1 for v in [supplier, number, date, tot] if v is not None)
    confidence = round(found / 4, 2)

    return {
        "supplier": supplier,
        "supplier_vat_id": vat_id,
        "supplier_tax_code": tax_code,
        "account_holder": account_holder,
        "number": number,
        "date": date,
        "net_amount": net,
        "vat_amount": vat,
        "total_amount": tot,
        "currency": currency,
        "confidence": confidence,
    }


# ============================== PARSER PREVENTIVO ==============================
def parse_quote(text: str) -> Dict[str, Any]:
    customer = None
    for pat in [r"cliente\s*:\s*(.+)", r"spett\.le\s*(.+)", r"\bto\s*:\s*(.+)"]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            customer = _clean_name(m.group(1))
            break

    valid_until = _find_date(text)

    m = re.search(r"\btotale\b[^\d]*(" + NUM_RE + ")", text.lower())
    total = _to_float_eu(m.group(1)) if m else None

    items = []
    for ln in text.splitlines():
        if re.search(r"\b(x|qty|q\.tà|qta|quantità)\b", ln.lower()):
            items.append(ln.strip())

    confidence = round(sum(1 for v in [customer, total] if v is not None) / 2, 2)

    return {
        "customer": customer,
        "valid_until": valid_until,
        "total_amount": total,
        "items_json": ("\n".join(items) if items else None),
        "confidence": confidence,
    }

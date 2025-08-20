from typing import Tuple

KEYS = {
    "fattura": ["fattura", "invoice", "iva", "partita iva", "imponibile"],
    "preventivo": ["preventivo", "offerta", "quotazione", "validità offerta"],
    "pec": ["posta elettronica certificata", "pec", "ricevuta di accettazione"],
}

ORDER = ["fattura", "preventivo", "pec"]

def classify_text(text: str) -> Tuple[str, float]:
    text_l = text.lower()
    scores = {}
    for label, words in KEYS.items():
        hit = sum(1 for w in words if w in text_l)
        # confidenza grezza: match/len(parole chiave) (clippata 0..1)
        scores[label] = min(1.0, hit / max(1, len(words)))
    # label con score più alto
    label = max(scores, key=scores.get)
    conf = scores[label]
    # fallback se tutto zero
    if conf == 0.0:
        return "altro", 0.3
    return label, float(round(conf, 2))
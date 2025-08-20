import os
from typing import Tuple
import pytesseract
from pdf2image import convert_from_path
from pypdf import PdfReader
from PIL import Image
import shutil

IMG_EXT = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".webp"}

def _ocr_image(img: Image.Image) -> str:
    return pytesseract.image_to_string(img, lang="ita+eng")

def extract_text(file_path: str) -> Tuple[str, bool]:
    """Ritorna (testo, used_ocr). Esegue OCR se serve."""
    ext = os.path.splitext(file_path)[1].lower()
    # Immagini: OCR diretto
    if ext in IMG_EXT:
        with Image.open(file_path) as im:
            return _ocr_image(im), True
    # PDF: prova testo nativo
    if ext == ".pdf":
        try:
            reader = PdfReader(file_path)
            text = "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception:
            text = ""
        # Se poco testo, usa OCR
        if len(text.strip()) < 100:
            pages = convert_from_path(file_path, dpi=300)
            ocr_texts = [_ocr_image(p) for p in pages]
            return "\n".join(ocr_texts), True
        return text, False
    # Altri formati: nessuna estrazione
    return "", False

tcmd = shutil.which("tesseract") or "/opt/homebrew/bin/tesseract"
pytesseract.pytesseract.tesseract_cmd = tcmd
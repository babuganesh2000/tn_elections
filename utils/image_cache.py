"""Local candidate image cache helpers."""

from pathlib import Path
from urllib.parse import urlparse


IMAGE_DIR = Path(__file__).parent.parent / "assets" / "candidate_images"
KNOWN_EXTENSIONS = [".jpg", ".jpeg", ".png", ".webp", ".svg"]


def ensure_image_dir():
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    return IMAGE_DIR


def _extension_from_url(photo_url):
    parsed = urlparse(str(photo_url))
    suffix = Path(parsed.path).suffix.lower()
    return suffix if suffix in KNOWN_EXTENSIONS else ".jpg"


def expected_image_path(candidate_id, photo_url=None):
    ensure_image_dir()
    ext = _extension_from_url(photo_url)
    return IMAGE_DIR / f"{candidate_id}{ext}"


def find_local_image(candidate_id):
    ensure_image_dir()
    for ext in KNOWN_EXTENSIONS:
        path = IMAGE_DIR / f"{candidate_id}{ext}"
        if path.exists():
            return path
    return None

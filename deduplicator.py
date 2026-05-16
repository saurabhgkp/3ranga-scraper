import hashlib


def job_hash(title: str, company: str, location: str) -> str:
    """SHA-256 of normalised (title + company + location)."""
    raw = f"{title.lower().strip()}|{company.lower().strip()}|{location.lower().strip()}"
    return hashlib.sha256(raw.encode()).hexdigest()

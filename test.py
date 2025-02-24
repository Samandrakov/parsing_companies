from urllib.parse import urlparse

def extract_language_subdomain(url):
    parsed_url = urlparse(url)
    netloc = parsed_url.netloc
    parts = netloc.split('.')

    if len(parts) > 2 and len(parts[0]) in (2, 3):
        return parts[0]
    else:
        return None


url = "https://ru.wikipedia.org/wiki/%D0%9F%D1%91%D1%82%D1%80%20III"
language_subdomain = extract_language_subdomain(url)
print(language_subdomain)

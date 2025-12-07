import urllib.parse

def extract_direct_url(redirect_url: str) -> str:
    """
    Extracts and decodes the direct URL from a Google redirect URL.
    Returns the cleaned URL, or the original if parsing fails.
    """
    try:
        # Parse URL
        parsed = urllib.parse.urlparse(redirect_url)
        params = urllib.parse.parse_qs(parsed.query)

        # Google redirect links store the real URL in the "url" parameter
        if "url" in params:
            clean_url = params["url"][0]
            # Decode URL encoding
            return urllib.parse.unquote(clean_url)
        else:
            return redirect_url
    except Exception:
        return redirect_url


def convert_links(input_file: str, output_file: str):
    cleaned_links = []

    with open(input_file, "r", encoding="utf-8") as f:
        redirect_links = f.read().splitlines()

    for link in redirect_links:
        cleaned_links.append(extract_direct_url(link))

    with open(output_file, "w", encoding="utf-8") as f:
        for url in cleaned_links:
            f.write(url + "\n")

    print(f"Done! Clean links written to {output_file}")


# Example usage:
convert_links("redirecting.txt", "clean_links.txt")

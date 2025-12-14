def onedrive_direct_link(share_link):
    """
    Convert a OneDrive sharing link to a direct download link suitable for <img> tag.
    """
    import re
    import urllib.parse

    # Extract the `id` parameter from the link
    parsed = urllib.parse.urlparse(share_link)
    query = urllib.parse.parse_qs(parsed.query)

    if 'id' in query:
        file_id = query['id'][0]
        # Construct a direct download URL
        direct_link = f"https://onedrive.live.com/download?resid={file_id}"
        return direct_link
    else:
        # Fallback: try to parse from the share URL using regex
        match = re.search(r'!(\w+)', share_link)
        if match:
            file_id = match.group(1)
            return f"https://onedrive.live.com/download?resid={file_id}"
        else:
            raise ValueError("Cannot extract file ID from link")

# Example usage:
share_link = "https://onedrive.live.com/?qt=allmyphotos&photosData=%2Fshare%2F611CDC0D713715DB%21s09ce2c4537ef45eb8c74db1c12317d26%3Fithint%3Dphoto%26e%3D3IEf96%26migratedtospo%3Dtrue&cid=611CDC0D713715DB&id=611CDC0D713715DB%21s09ce2c4537ef45eb8c74db1c12317d26&redeem=aHR0cHM6Ly8xZHJ2Lm1zL2kvYy82MTFjZGMwZDcxMzcxNWRiL0VVVXN6Z252Ti10RmpIVGJIQkl4ZlNZQmZkVFJiR1BnNFFKcnpYUXBFSURfclE%5FZT0zSUVmOTY&v=photos"

direct_link = onedrive_direct_link(share_link)
print(direct_link)

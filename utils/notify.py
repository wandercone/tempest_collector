import requests

def send_gotify(
    title,
    message,
    priority,
    gotify_url,
    app_token,
    markdown=True,
    timeout=10,
    click_url=None,
    image_url=None,
):
    """
    Send a notification to a Gotify server.

    Returns:
        (True, response_json) on success
        (False, error_message) on failure
    """
    # Validate inputs
    if not (0 <= priority <= 10):
        raise ValueError(f"Priority must be between 0-10 (got {priority})")

    required = {
        "title": title,
        "message": message,
        "gotify_url": gotify_url,
        "app_token": app_token,
    }

    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    # Normalize URL
    if not gotify_url.startswith(("http://", "https://")):
        gotify_url = f"https://{gotify_url}"

    endpoint = f"{gotify_url.rstrip('/')}/message"

    # Enable markdown automatically for images
    if image_url:
        markdown = True
        message += f"\n\n![image]({image_url})"

    payload = {
        "title": title,
        "message": message,
        "priority": priority,
    }

    extras = {}

    if markdown:
        extras.setdefault("client::display", {})["contentType"] = "text/markdown"

    if click_url:
        extras.setdefault("client::notification", {})["click"] = {"url": click_url}

    if extras:
        payload["extras"] = extras

    headers = {"X-Gotify-Key": app_token}

    try:
        r = requests.post(endpoint, json=payload, headers=headers, timeout=timeout)
        r.raise_for_status()
        return True, r.json()

    except requests.exceptions.Timeout:
        return False, f"Request timed out after {timeout}s"

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else "unknown"

        errors = {
            400: "Bad request - invalid payload",
            401: "Authentication failed - check app token",
            403: "Forbidden - token may lack permission",
            404: "Gotify endpoint not found",
        }

        return False, errors.get(status, f"HTTP error {status}")

    except requests.exceptions.ConnectionError:
        return False, f"Cannot reach {gotify_url}"

    except Exception as e:
        return False, f"Unexpected error: {e}"
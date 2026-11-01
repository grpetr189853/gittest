import re

def validate_email(email: str) -> bool:
    """
    Validates an email address using a regex pattern.

    Args:
        email (str): The email address to validate.

    Returns:
        bool: True if the email address is valid, False otherwise.
    """
    # Regex pattern for validating email addresses
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    
    # Match the email against the regex pattern
    return bool(re.match(email_regex, email))

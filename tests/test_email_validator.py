import unittest
from email_validator import validate_email

class TestEmailValidator(unittest.TestCase):
    def test_valid_emails(self):
        self.assertTrue(validate_email("test@example.com"))
        self.assertTrue(validate_email("user.name+tag+sorting@example.com"))
        self.assertTrue(validate_email("user_name@example.co.uk"))
        self.assertTrue(validate_email("user-name@sub.example.com"))
        self.assertTrue(validate_email("user@123.123.123.123"))
        self.assertTrue(validate_email("user@[IPv6:2001:db8::1]"))

    def test_invalid_emails(self):
        self.assertFalse(validate_email("plainaddress"))
        self.assertFalse(validate_email("@missingusername.com"))
        self.assertFalse(validate_email("username@.com"))
        self.assertFalse(validate_email("username@.com."))
        self.assertFalse(validate_email("username@com"))
        self.assertFalse(validate_email("username@-example.com"))
        self.assertFalse(validate_email("username@example..com"))
        self.assertFalse(validate_email("username@.example.com"))
        self.assertFalse(validate_email("username@.example..com"))

if __name__ == "__main__":
    unittest.main()

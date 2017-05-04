import unittest

import jwt

from baseplate import authn


RSA_PRIVATE_KEY = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA5elqJP6lvR2PbDVnT+QdE+sSy5Dae8sV9U0gT5LyBclFFlnZ
x2U0e2B2YKHNRdhNwvnVU/DPObGLwQvGHkxkl8UJ4yxYjtVBDsULzzoI8E4qK+aQ
J2dwf00WBqH44+ZZn3ZqUs8D5BZDX7nTYhAVJlc0hH8GwRaVz+q3BnuomDKHYgLz
sjF4LD8mRwDoEucy15vmjqpyXltTfRw4eEpJYx/BLWt217u+XqawxHHv6HGZEU1d
r5Yh0bTQF+ew00H5bjx+6yd9aDBAEoiE+FPJOnvr3nIYf4S27fRXV6OzjZLqnpK+
vaifJuMPwVpW+upZF0T6+SbnmhU/KE1D5JMp+QIDAQABAoIBAQCMrd9yFZMKfdz1
hFPb8aPNPUi54L+fgevEtlWv+yU2Xypz+7SjKo0LdUHZ7Qdy1mD2jfJ3s2DJV5dB
H1gxz+K5byqWo9roQxrU08NfII65o8pwJFtOkR+n9V4l3tQxdxCd31I9q0ghN3Iw
T79FQLwAQtnyvNtAKPawS1mEkQPfY9qllK9UmcKqNr15HJbrziA5NO9vqDNmAdt1
/xUzgNjUDXx+OH5TSYiGbXUJEqUBla5UHDjSHuIsz5s2wbMO8w23KGEu2UIbt0e1
vSA8V6VsWVjJP60BReRFQBoLEA9R2qJIJ7YCSJSXAYK7qZQdFAAi7DL0TJne0btd
W0CRsAkBAoGBAPn2Gz+wR9bTbMi6+GHB766FMnhbc99IvdNk2jCRkP5QMnkT9zOr
kevrDXr5bxchN5UfQlwJ6HkMzG3+V4neoxjTuOH7MdQ3jN7b/txO/MVmMcqHj5Bt
Y0ssn0A/4kcGMDf7Nf5qtLDROWkcZInc757rBpFj89R2MPzSaflf8ZQZAoGBAOt3
T5uWWZu0KoW9qUOiVeO9WoUe69AEalmH2eBoJnmCy8aKWmkbX+n1DqurAR9lL1rZ
v7I8ZBIEChIeysOFwERmxAl33p8q3v7/Na3V+Gnp9t1KMNdJ6d8NtpBVm7snPPsg
4dkF90Oh3c8VzLWqOEKVhk1WXAN1bdmH4EdDQADhAoGAT5KQJBs7E2Fk7RXQQlaZ
tYYDhhse7QLcIzKk3vdIIB1po+++Lz28C01dkjtbqRJS/m7gEiq5TefMIGvqdCJi
62FjJtVvjG5Osxd3r6yZfUHGMgIrqr+X73N6EtsDbrbCnN+k1aQkd3FzhEmcm+Kw
iXeqNJiMI6OofqOBRvjLD+kCgYANAZYgHfntI9KFeKh01+6umbL6T27vGo9VSq+6
OYaGh7cBzHqZ+60cmzCoQtXZChnHhueTzMoqRJbZ0WGZ0zV/kb2aWEivugp/Q1GP
vJXwI8BDEOoJukd0zqka59+mQtCXfoV7G11BGxvJaIbaMgDRjLjSZMbIWiArN9on
lYtfgQKBgEIgTLN9wZWXr6mE+Qg2vj24V/hBaaM78fUxXq7TLw3b9D/GpXmcgFcz
vkVjktUJ+t9qGvuopqsUq4OjYy31Btw6XjYOMrhIWB3Emn7x/QYYLYh9TBEQSbg2
vsgT2KQ1ClOrJi9p9OM+HPLtBt5W3eUtkdpUIVfwVdzDU2dzy0Aq
-----END RSA PRIVATE KEY-----"""


RSA_PUBLIC_KEY = """-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEA5elqJP6lvR2PbDVnT+QdE+sSy5Dae8sV9U0gT5LyBclFFlnZx2U0
e2B2YKHNRdhNwvnVU/DPObGLwQvGHkxkl8UJ4yxYjtVBDsULzzoI8E4qK+aQJ2dw
f00WBqH44+ZZn3ZqUs8D5BZDX7nTYhAVJlc0hH8GwRaVz+q3BnuomDKHYgLzsjF4
LD8mRwDoEucy15vmjqpyXltTfRw4eEpJYx/BLWt217u+XqawxHHv6HGZEU1dr5Yh
0bTQF+ew00H5bjx+6yd9aDBAEoiE+FPJOnvr3nIYf4S27fRXV6OzjZLqnpK+vaif
JuMPwVpW+upZF0T6+SbnmhU/KE1D5JMp+QIDAQAB
-----END RSA PUBLIC KEY-----"""


class JwtTests(unittest.TestCase):
    def test_is_authorized_for_sub_hs_256(self):
        jwt_token = jwt.encode({"sub": "foo"}, key="secret", algorithm="HS256")
        self.assertTrue(authn.is_authorized_for_sub(
            jwt_token, key="secret", sub="foo"))

    def test_is_authorized_for_sub_rs_256(self):
        jwt_token = jwt.encode(
            {"sub": "foo"}, key=RSA_PRIVATE_KEY, algorithm="RS256")
        self.assertTrue(authn.is_authorized_for_sub(
            jwt_token, key=RSA_PUBLIC_KEY, sub="foo"))

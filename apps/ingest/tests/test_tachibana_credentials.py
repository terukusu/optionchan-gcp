import pytest
from tachibana_fakes import AUTH_ID, PASSWORD, PRIVATE_KEY

from optionchan_ingest.sources.tachibana.credentials import TachibanaCredentials


def test_repr_and_str_hide_values():
    creds = TachibanaCredentials(auth_id=AUTH_ID, private_key=PRIVATE_KEY, password=PASSWORD)
    texts = [repr(creds), str(creds), f"{creds}", f"{creds!r}", format(creds, ""), repr([creds]), repr({"c": creds})]
    for text in texts:
        for secret in (AUTH_ID, PRIVATE_KEY, PASSWORD, "SECRET-"):
            assert secret not in text
    assert repr(creds) == "TachibanaCredentials(<redacted>)"


def test_values_are_kept():
    creds = TachibanaCredentials(auth_id=AUTH_ID, private_key=PRIVATE_KEY.encode())
    assert creds.auth_id == AUTH_ID
    assert creds.private_key == PRIVATE_KEY.encode()
    assert creds.password is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"auth_id": "", "private_key": PRIVATE_KEY},
        {"auth_id": AUTH_ID, "private_key": ""},
        {"auth_id": AUTH_ID, "private_key": b""},
        {"auth_id": AUTH_ID, "private_key": PRIVATE_KEY, "password": ""},
    ],
)
def test_empty_values_are_rejected_without_values_in_message(kwargs):
    with pytest.raises(ValueError) as e:
        TachibanaCredentials(**kwargs)
    assert "SECRET-" not in str(e.value)

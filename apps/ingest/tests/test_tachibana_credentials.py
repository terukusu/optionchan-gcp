import pytest
from tachibana_fakes import AUTH_ID, PRIVATE_KEY

from optionchan_ingest.sources.tachibana import credentials as tcred
from optionchan_ingest.sources.tachibana.credentials import TachibanaCredentials


def test_repr_and_str_hide_values():
    creds = TachibanaCredentials(auth_id=AUTH_ID, private_key=PRIVATE_KEY)
    texts = [repr(creds), str(creds), f"{creds}", f"{creds!r}", format(creds, ""), repr([creds]), repr({"c": creds})]
    for text in texts:
        for secret in (AUTH_ID, PRIVATE_KEY, "SECRET-"):
            assert secret not in text
    assert repr(creds) == "TachibanaCredentials(<redacted>)"


def test_values_are_kept():
    creds = TachibanaCredentials(auth_id=AUTH_ID, private_key=PRIVATE_KEY.encode())
    assert creds.auth_id == AUTH_ID
    assert creds.private_key == PRIVATE_KEY.encode()


def test_password_is_not_accepted():
    # ログインの項目は sAuthId だけ（資料で確かめた）。パスワードの引数は無い
    with pytest.raises(TypeError):
        TachibanaCredentials(auth_id=AUTH_ID, private_key=PRIVATE_KEY, password="x")
    assert not hasattr(TachibanaCredentials(auth_id=AUTH_ID, private_key=PRIVATE_KEY), "password")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"auth_id": "", "private_key": PRIVATE_KEY},
        {"auth_id": AUTH_ID, "private_key": ""},
        {"auth_id": AUTH_ID, "private_key": b""},
    ],
)
def test_empty_values_are_rejected_without_values_in_message(kwargs):
    with pytest.raises(ValueError) as e:
        TachibanaCredentials(**kwargs)
    assert "SECRET-" not in str(e.value)


def test_virtual_url_decryption_spec_is_fixed():
    # 復号の仕様（資料）。実装は段 2（cryptography を pyproject に直接書いてから）
    assert tcred.VIRTUAL_URL_KEY_BITS == (2048, 4096)
    assert tcred.VIRTUAL_URL_OAEP_HASH == "SHA-256"

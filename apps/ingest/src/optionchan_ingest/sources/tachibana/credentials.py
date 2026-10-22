"""立花証券 e 支店 API の認証情報の値の型と、仮想 URL の復号の差し込み口。

値は引数で受け取る（環境変数・ファイルは読まない。Secret Manager から作る関数は段 2 で足す）。
repr・str・例外の文に値を出さない。

認証 ID（sAuthId）の出どころ: 電話認証は v4r9 で廃止された。認証 ID は e 支店の利用設定の画面で自動で作られ、
ダウンロードして使う（標準 Web へのパスキーの登録が前提）。ログインの項目は sAuthId だけで、パスワードは無い。
"""

from collections.abc import Callable
from dataclasses import dataclass, field

# 暗号化された仮想 URL（ログインの応答の sUrl*）→ 復号した URL。
# 登録した公開鍵でサーバーが暗号化し、対の秘密鍵で復号する。仕様（資料で確かめた）:
# 入力は base64、RSA（VIRTUAL_URL_KEY_BITS のどちらか）の OAEP（ハッシュ・MGF1 とも VIRTUAL_URL_OAEP_HASH）、
# 出力は ASCII の URL で末尾に改行（"\r\n"）が付くので落とす（client も落とす）。
# 実装は段 2。暗号ライブラリ（cryptography）は今は google-auth の依存として入っているだけなので、
# pyproject に直接書いてから実装する。この Issue では呼ぶ側が関数を渡す
Decryptor = Callable[[str], str]
VIRTUAL_URL_KEY_BITS = (2048, 4096)
VIRTUAL_URL_OAEP_HASH = "SHA-256"

_REDACTED = "TachibanaCredentials(<redacted>)"


@dataclass(frozen=True)
class TachibanaCredentials:
    """認証 ID（利用設定の画面で作られたもの）・秘密鍵（仮想 URL の復号用）。

    private_key は段 2 の Decryptor を作るのに使う（client は使わない）
    """

    auth_id: str = field(repr=False)
    private_key: str | bytes = field(repr=False)

    def __post_init__(self) -> None:
        # 文に値を入れない（項目名だけ）
        if not self.auth_id:
            raise ValueError("auth_id is empty")
        if not self.private_key:
            raise ValueError("private_key is empty")

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__

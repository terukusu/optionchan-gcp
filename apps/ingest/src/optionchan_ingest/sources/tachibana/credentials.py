"""立花証券 e 支店 API の認証情報の値の型と、仮想 URL の復号の差し込み口。

値は引数で受け取る（環境変数・ファイルは読まない。Secret Manager から作る関数は段 2 で足す）。
repr・str・例外の文に値を出さない。
"""

from collections.abc import Callable
from dataclasses import dataclass, field

# 暗号化された仮想 URL（ログインの応答の sUrl*）→ 復号した URL。
# 登録した公開鍵でサーバーが暗号化し、対の秘密鍵で復号する。暗号方式（RSA の鍵長・パディング・符号化）が未確認なので、
# 実装と暗号ライブラリの追加は段 2。この Issue では呼ぶ側が関数を渡す
Decryptor = Callable[[str], str]

_REDACTED = "TachibanaCredentials(<redacted>)"


@dataclass(frozen=True)
class TachibanaCredentials:
    """認証 ID（電話認証で発行されたもの）・秘密鍵（仮想 URL の復号用）・パスワード（任意）。

    v4r10 のリファレンスにはログインのパスワードの項目が見当たらない（未確認）ので任意にした。
    private_key は段 2 の Decryptor を作るのに使う（client は使わない）
    """

    auth_id: str = field(repr=False)
    private_key: str | bytes = field(repr=False)
    password: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        # 文に値を入れない（項目名だけ）
        if not self.auth_id:
            raise ValueError("auth_id is empty")
        if not self.private_key:
            raise ValueError("private_key is empty")
        if self.password is not None and not self.password:
            raise ValueError("password is empty")

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__

import logging


def getLogger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # ローカルでのデバッグ用

    # ハンドラーが既に存在する場合は追加しない
    if not logger.handlers:
        # Cloud Functions環境に適したハンドラー設定
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

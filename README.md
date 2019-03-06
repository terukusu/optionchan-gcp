# optionchan for GCP
日経225オプションの価格情報を蓄積していくツール。の、Google Cloud Platform版
Cloud Functionsとかでガーッと価格情報を取ってきて、BigQueryにヴァッと蓄積していきます。

* appengine
  * ビューアー用のウェブアプリ。とりあえず Chart.js でATMのIV推移と直近のスマイルカーブを可視化している

* functions
  * ４個の Cloud Functionが含まれる 
    * データ取り込み用
      * download_jpx
        * スクレーピング → json化 → Cloud Storageへ保存するもの
      * load_jpx_into_bq
        * Cloud Storage へ保存されたJSONをBigQueryに読み込むもの
    * データ表示用
      * atm_data
        * ATM オプションの IV 推移表示用のデータを返す WebAPI
      * smile_data
        * 直近のスマイルカーブ表示用のデータを返す WebAPI

* js_functions
  * 細々とした Cloud Functionsが入ってる
    * Stackdriverから受け取った通知をLineへ通知する関数

* schema
  * BigQueryのスキーマ

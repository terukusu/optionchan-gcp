# stackdriverLineNotify  
Stackdriverから通知を受けとるためのWebhook用 Cloud Function。受け取った情報は整形してLineに通知する。
アクセス元を認証するために、クエリ文字列の access_token パラメータをチェックします。

# 準備
↓ の内容で auth.txt を準備する。  

```
{
  "access_token": "aaaaaaaaaaaaaaaa",
  "line_token": "bbbbbbbbbbbbbbbb",
  "line_user_id": "cccccccccccccccc"
}

```

* access_token
  * Stackdriver からクエリ文字列で送る access_token パラメータ  
* line_token
  * Line Messaging API にアクセスするためのトークン。 Line Developers Console から取得しておく。  
* line_user_id
  * Lineで通知を受け取るユーザーのLine ID。こういうやつ→ Ussj06e64256ebecebb5c53ecebebb5eb87af  

# デプロイ
```
gcloud functions deploy stackDriverLineNotify --runtime=nodejs8 --memory=128 --region=us-ea
st1 --trigger-http
```

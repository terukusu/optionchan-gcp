const request = require('request');

const auth = require('./auth.json');
const config = require('./config.json');

exports.stackdriverLineNotify = (req, res) => {
  let auth_token = req.query.auth_token;

  if (!auth_token || auth_token !== auth.auth_token) {
    res.status(403).send('Forbidden');
    return;
  }

  // Stackdriverからのメッセージ
  let sd_json = req.body;
  let i = sd_json.incident;

  let message = '【SVR1】Stackdriverからのお知らせでです。\n'
    + '状況: ' + i.state + '\n'
    + '対象: ' + i.resource_name + '\n'
    + 'ポリシー: ' + i.policy_name + '\n'
    + '発生日時: ' + getJstMmddhhmm(i.started_at) + '\n'
    + '収束日時: ' + getJstMmddhhmm(i.ended_at) + '\n'
    + 'サマリ: ' + i.summary + '\n'
    + '詳細: ' + i.url;

  var options = {
    uri: config.line_push_url,
    headers: {
      'Content-type': 'application/json; charset=utf-8',
      'Authorization': 'Bearer ' + auth.line_token
    },
    json: {
      'to': auth.line_user_id,
      "messages": [{
        'type': 'text',
        'text': message
      }]
    }
  };

  // Line へメッセージ送信！
  request.post(options, (e, r, b) => {
    if (e || r.statusCode != 200) {
      console.log(e + r + b);
    }
  });

  res.status(200).send('OK');
};

function getJstMmddhhmm(unixtime) {

  if (!unixtime) {
    return '';
  }

  let d = new Date((unixtime + (3600 * 9)) * 1000);

  let month = d.getUTCMonth() + 1;
  let date = d.getUTCDate();
  let hour = d.getUTCHours();
  let minute = d.getUTCMinutes();

  let date_str = month + '/' + date + ' ' + hour + ':' + minute;

  return date_str;
}

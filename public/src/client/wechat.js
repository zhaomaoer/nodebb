
var OAuth = require('wechat-oauth');
var client = new OAuth('wx0dc7749837133b06', 'd4624c36b6795d1d99dcf0547af5443d');

  client.getAccessToken('oAw88wDVi1fzCevvQkMwMmG3n6qE', function (err, result) {
  var accessToken = result.data.access_token;
  var openid = result.data.openid;
  console.log("accessToken:"+accessToken);
  console.log("openid:"+openid)
});
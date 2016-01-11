"use strict";
/* global define, app, config, RELATIVE_PATH */


define('forum/login', ['csrf', 'translator'], function(csrf, translator) {
	 
	var	Login = {};

	function getUrlParam(name)
	{
		var reg = new RegExp("(^|&)"+ name +"=([^&]*)(&|$)"); //构造一个含有目标参数的正则表达式对象
		var r = window.location.search.substr(1).match(reg);  //匹配目标参数
		if (r!=null) return unescape(r[2]); return null; //返回参数值
	} 
	
	

	Login.init = function() {
		var wechat=getUrlParam('page');
		var errorEl = $('#login-error-notify'),
			submitEl = $('#login'),
			formEl = $('#login-form');

		submitEl.on('click', function(e) {

			e.preventDefault();
			
			if (!$('#username').val() || !$('#password').val()) {
				errorEl.find('p').translateText('[[error:invalid-username-or-password]]');
				errorEl.show();
			} else {
				errorEl.hide();

				if (submitEl.hasClass('disabled')) {
					return;
				}

				submitEl.addClass('disabled');
				formEl.ajaxSubmit({
					headers: {
						'x-csrf-token': csrf.get()
					},
					success: function(data, status) {
						window.location.href = data;
					},
					error: function(data, status) {
						errorEl.find('p').translateText(data.responseText);
						errorEl.show();
						submitEl.removeClass('disabled');
						
					}
				});
			}
		});
			$('#login-error-notify button').on('click', function(e) {
			e.preventDefault();
			errorEl.hide();
			return false;
		});
		if(wechat){
		$('#login-all').hide();
		submitEl.trigger('click');
		}else{
		$('#login-all').show();
		}
		/*if ($('#username').val() &&$('#password').val()){
		$('#login-all').hide();
		submitEl.trigger('click');
		}*/

		$('#content #username').focus();

		// Add "returnTo" data if present
		if (app.previousUrl) {
			var returnToEl = document.createElement('input');
			returnToEl.type = 'hidden';
			returnToEl.name = 'returnTo';
			returnToEl.value = app.previousUrl;
			$(returnToEl).appendTo(formEl);
		}
	};
		

	return Login;
});

/**
 * @author xialeistudio
 * @date 2016/5/19 0019
 */
'use strict';
const URL = require('url');
var url = '/user/info?product=qun&code=0f93UgGwCUHhl4d8fbbP4MOaTx3lCcVUDwX1iAWq-Qu6zJE8hc6u3evNEdGYaFyqn1rN2msYrX5-';
url = URL.parse(url);
var link = url.pathname;
if (url.query !== null) {
	var params = url.query.split('&');
	params = params.map(function(item) {
		var temp = item.split('=');
		return temp[0] + '=';
	});
	link += '?' + params.join('&');
}
console.log(link);
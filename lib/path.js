/**
 * Created by rices on 16/6/14.
 */
'use strict';
/**
 * { __time__: 1465747797,
  __topic__: '',
  __source__: '10.117.220.231',
  ip: '10.159.47.116',
  time: '13/Jun/2016:00:09:57 +0800',
  met: 'HEAD',
  url: '/api/update/check',
  status: '200',
  size: '0',
  ref: '-',
  ua: '-',
  host: '-',
  rtime: '0.016' }
 */

// var pathTongji = {};
var fs = require('fs');
var Promise = require('bluebird');
var tool = require('./_tool');
var _ = require('underscore');
var crypto = require('crypto');

var basePath = __dirname + '/../';
var downloadPath = '';
var urlMap = {};

Promise.promisifyAll(fs);
var excludedUrl = [
		'/api/update/check',
		'/v1/reddot/all',
		'/v1/card/getdefaultcard',
		'/v1/user/simple?id=',
		'/v1/reddot/clear',
		'.png',
		'.jpg',
		'l/*',
		'.gif',
		'/v1/app/jsapi'
];

/**
 * 过滤URL
 */
function urlFilter(url)
{
	url = url.replace(/^\/b\/[0-9a-zA-Z=_-]+/g, '/phonebook/*');
	url = url.replace(/^\/event\/[0-9a-zA-Z=_-]+/g, '/activity/*');
	url = url.replace(/^\/(b|phonebook|l|activity|phonebook-setting|event|news|card|family|forum)\/[0-9a-zA-Z=_-]+/g, '/$1/*');
	url = url.replace(/\/(s|code|add|topic|chat)\/[0-9a-zA-Z=_-]+/g, '/$1/*');
	url = url.replace(/=([0-9a-zA-Z\-_%:\.]+)/g, '=');
	url = url.replace(/(\?|&)(keyword|page|offset|cursor|from|max|isappinstalled)=/g, '');
	url = url.replace(/(\?)/g, '&');
	url = url.replace(/=/g, '');
	_.each(excludedUrl, function(u) {
		if(url.indexOf(u) >= 0){
			url = '';
		}
	});
	// url = url.replace('/', '');
	url = url.replace(' ', '');
	return url;
}
module.exports.init = function(){
	downloadPath = basePath + 'path/';
	return tool.rmPath(downloadPath).then(function(){
		return tool.initPath(downloadPath).then(function() {
			console.log('OK');
		});
	});
};

function run(json, yesterdayStr){
	var url = urlFilter(json.url);
	if(url == "") return;
	var date = new Date(json.time.replace(/(20\d{2}):/g, '$1 '));
	var ip = json.ip.replace(/\./g, '-');

	var key = crypto.createHash('md5').update(url).digest('hex').substring(0,6);
	urlMap[key] = url;

	fs.appendFile(downloadPath+'/'+ip+'.txt', key+'|'+date.getTime()/1000+"\n");
}
module.exports.run = run;
module.exports.saveMap = function(callback) {
	fs.appendFile(downloadPath+'/map.txt', JSON.stringify(urlMap), callback);
};
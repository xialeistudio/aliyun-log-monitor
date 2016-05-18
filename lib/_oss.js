/**
 * oss操作
 * @author xialeistudio
 * @date 2016/5/18 0018
 */
'use strict';
var OSS = require('ali-oss');
var co = require('co');
var EventEmitter = require('events').EventEmitter;
var config = require('../config.json').oss;
var client = new OSS({
	region: config.region,
	accessKeyId: config.accessKeyID,
	accessKeySecret: config.accessKeySecret
});
/**
 * 获取原始文件名
 * @param name
 * @returns {*}
 */
function getBaseName(name) {
	var names = name.split('/');
	return names[names.length - 1];
}
/**
 * 下载objects
 * @param path 本地路径
 * @param ossOptions
 * @returns {*|EventEmitter}
 */
function download(path, ossOptions) {
	client.useBucket(config.bucket);
	var ee = new EventEmitter();
	co(function*() {
		var result = yield client.list(ossOptions);
		ee.emit('list', result);
		var total = result.objects.length, downloaded = 0;
		result.objects.forEach(function(object) {
			var baseName = getBaseName(object.name);
			co(function*() {
				yield client.get(object.name, path + '/' + baseName);
				downloaded++;
				ee.emit('object', object.url, path + '/' + baseName, downloaded, total);
				if (downloaded === total) {
					ee.emit('end');
				}
			}).catch(function(e) {
				ee.emit('error', e);
				downloaded++;
				if (downloaded === total) {
					ee.emit('end');
				}
			});
		});
	}).catch(function(e) {
		ee.emit('error', e);
	});
	return ee;
}
exports.download = download;
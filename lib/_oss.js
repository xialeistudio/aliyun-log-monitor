/**
 * oss操作
 * @author xialeistudio
 * @date 2016/5/18 0018
 */
'use strict';
var OSS = require('ali-oss');
var co = require('co');
var EventEmitter = require('events').EventEmitter;
var Promise = require('bluebird');
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
		if (result.objects === undefined) {
			ee.emit('listEmpty');
		}
		else {
			ee.emit('listSuccess', result);
			var downloaded = 0, total = result.objects.length;
			//监听内部事件
			ee.on('_download', function(index) {
				var object = result.objects[index];
				co(function*() {
					yield client.get(object.name, path + '/' + getBaseName(object.name));
					downloaded++;
					//触发外部事件
					ee.emit('objectSuccess', object.name, path + '/' + getBaseName(object.name), downloaded, total);
					//触发下一个
					if (downloaded < total) {
						ee.emit('_download', downloaded);
					}
					else {
						ee.emit('downloadComplete');
					}
				}).catch(function(e) {
					downloaded++;
					ee.emit('objectError', e, downloaded, total);
					if (downloaded < total) {
						ee.emit('_download', downloaded);
					}
					else {
						ee.emit('downloadComplete');
					}
				});
			});
			//触发第一次下载
			process.nextTick(function() {
				ee.emit('_download',downloaded);
			});
		}
	}).catch(function(e) {
		ee.emit('listError', e);
	});
	return ee;
}
/**
 * 删除object
 * @param name
 */
function remove(name) {
	return new Promise(function(resolve, reject) {
		client.useBucket(config.bucket);
		co(function*() {
			var result = yield client.delete(name);
			resolve(result);
		}).catch(reject);
	});
}
exports.download = download;
exports.remove = remove;
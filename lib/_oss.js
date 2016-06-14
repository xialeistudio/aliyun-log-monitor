/**
 * oss操作
 * @author xialeistudio
 * @date 2016/5/18 0018
 */
'use strict';
var OSS = require('ali-oss');
var co = require('co');
var fs = require('fs');
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

	ee.on('_download', function(object) {
		co(function*() {
			if(!fs.existsSync(path + '/' + getBaseName(object.name))){
				yield client.get(object.name, path + '/_' + getBaseName(object.name));
				fs.renameSync(path + '/_' + getBaseName(object.name), path + '/' + getBaseName(object.name));
			}else{
				// console.log(object.name+'文件已存在');
			}
			//触发外部事件
			ee.emit('objectSuccess', object.name, path + '/' + getBaseName(object.name));
		}).catch(function(e) {
			ee.emit('objectError', e, object);
		});
	});

	co(function*() {
		console.info('loading');
		var result = yield client.list(ossOptions);

		if (result.objects === undefined) {
			ee.emit('listEmpty');
		}
		else {
			ee.emit('listSuccess', result);
		}
	}).catch(function(e) {

		console.info(e);
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
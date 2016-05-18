/**
 * @author xialeistudio
 * @date 2016/5/18 0018
 */
'use strict';
var snappy = require('snappy');
var fs = require('fs');
var Promise = require('bluebird');
Promise.promisifyAll(fs);
Promise.promisifyAll(snappy);
/**
 * 解析文件
 * @param path
 * @param options
 */
function decodeFile(path, options) {
	//check exists
	return fs.existsAsync(path).then(function(exists) {
		if (!exists) {
			throw new Error(path + ' not exists!');
		}
		return snappy.uncompressAsync(path, options || {asBuffer: false});
	});
}
/**
 * 解析二进制数据
 * @param data
 * @param options
 */
function decodeBinary(data, options) {
	return snappy.uncompressAsync(data, options || {asBuffer: false});
}
/**
 * 压缩为文件
 * @param data
 * @param path
 */
function encodeToFile(data, path) {
	return snappy.compressAsync(data).then(function(compressedData) {
		return fs.writeFileAsync(path, compressedData);
	});
}
/**
 * 压缩为二进制数据
 * @param data
 * @returns {*}
 */
function encodeToBuffer(data) {
	return snappy.compressAsync(data);
}
exports.decodeFile = decodeFile;
exports.decodeBinary = decodeBinary;
exports.encodeToFile = encodeToFile;
exports.encodeToBuffer = encodeToBuffer;
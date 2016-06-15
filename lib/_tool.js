
'use strict';
var fs = require('fs');
var Promise = require('bluebird');
Promise.promisifyAll(fs);
module.exports = {};

/**
 * 初始化目录
 * @param path
 * @returns {Promise}
 */
function initPath(path) {
	return new Promise(function(resolve) {
		fs.exists(path, function(exists) {
			if (!exists) {
				resolve(fs.mkdirAsync(path));
			}
			else {
				resolve();
			}
		});
	}).then(function() {
		return path;
	});
}


function unlinkAllExcludPath(path, dirname) {
	var exec = require('child_process').exec, child;
	var shell = 'cd '+path+' && rm -rf `ls |egrep -v ' + dirname + '`';
	console.log(shell);

	return new Promise(function(resolve, reject) {
		child = exec(shell, function(err, out) {
			if (err) {
				reject(err);
			}
			else {
				resolve(out);
			}
		});
	});
}

module.exports.initPath = initPath;
module.exports.unlinkAllExcludPath = unlinkAllExcludPath;
module.exports.rmPath = function(path){
	var exec = require('child_process').exec, child;
	var shell = 'rm -rf '+path;
	return new Promise(function(resolve, reject) {
		child = exec(shell, function(err, out) {
			if (err) {
				reject(err);
			}
			else {
				resolve(out);
			}
		});
	});
};
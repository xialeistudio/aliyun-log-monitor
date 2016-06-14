/**
 * @author xialeistudio
 * @date 2016/5/18 0018
 */
'use strict';
var database = require('./lib/_database');
var logger = require('./lib/_logger');
var oss = require('./lib/_oss');
// var snappy = require('./lib/_snappy');
var Moment = require('moment');
var Promise = require('bluebird');
var fs = require('fs');
var config = require('./config.json');
var _ = require('underscore');
var URL = require('url');
Promise.promisifyAll(fs);
var lineReader = require('line-reader');
var co = require('co');
var crypto = require('crypto');
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
var dbData = [];//最终入库数据
var dbDataMap = {};//最终入库数据
/**
 * 数据入库
 * @param mysqlData
 * @returns {Array}
 */
function mergeDbData(mysqlData) {
	if (!dbData || dbData.length === 0) {
		dbData = [];
		dbDataMap = {};
	}
	var key = '_' + crypto.createHash('md5').update(mysqlData[0]).digest('hex');
	//检测mysqldata中的URL是否在dbdata中
	if (dbDataMap[key]) {
		var dbItem = dbData[dbDataMap[key]];
		dbItem[1] += mysqlData[1];
		dbItem[2] += mysqlData[2];
		dbItem[3] = parseFloat((dbItem[2] / dbItem[1]).toFixed(3));
		dbItem[4] = Math.min(dbItem[4], mysqlData[4]);
		dbItem[5] = Math.max(dbItem[5], mysqlData[5]);
	}
	else {
		dbDataMap[key] = dbData.length;
		dbData.push(mysqlData);
	}
}
/**
 * 解压出来的数据转换为Mysql数组【单行】
 * @param line
 * @param date
 * @returns {Array}
 */
function uncompressDataToMysqlData(line, date) {
	var json = JSON.parse(line);
	var url = json.url;
	var rtime = parseFloat(json.rtime);
	var status = parseInt(json.status);
	return [getUrlPattern(url), 1, rtime || 0, rtime || 0, rtime || 0, rtime || 0, date, status];
}
/**
 * 数据入库
 * @returns {*}
 */
function saveToDb() {
	var sql = 'INSERT INTO ' + config.mysql.tablePrefix + 'logs (url,requestCount,totalTime,averageTime,minTime,maxTime,date,status) VALUES ?';
	//MySQL批量插入
	return database().then(function(conn) {
		return conn.queryAsync(sql, [dbData]).then(function(res) {
			conn.destroy();
			return res;
		}).catch(function(e) {
			conn.destroy();
			throw e;
		});
	});
}
/**
 * 获取URL模式
 * @param url
 * @returns {string|null|string|*}
 */
function getUrlPattern(url) {
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
	return link;
}

function unlinkAllExcludPath(path, dirname) {
	var exec = require('child_process').exec, child;
	return new Promise(function(resolve, reject) {
		child = exec('rm -rf `ls '+path+'|egrep -v ' + dirname + '`', function(err, out) {
			if (err) {
				reject(err);
			}
			else {
				resolve(out);
			}
		});
	});
}
/**
 * 内存情况
 */
var showMem = function() {
	var mem = process.memoryUsage();
	var format = function(bytes) {
		return (bytes / 1024 / 1024).toFixed(2) + 'MB';
	};
	console.log('[memory] heapTotal: %s heapUsed: %s rss %s', format(mem.heapTotal), format(mem.heapUsed), format(mem.rss));
};
function tongji(fileList, yesterdayStr) {
	var readed = 0;
	var total = fileList.length;
	//读取文件，逐行
	var readFile = function() {
		var filePath = fileList.pop();
		var readedLine = 0;
		var promise = new Promise(function(resolve, reject) {
			lineReader.eachLine(filePath, function(line, last) {
				//处理行数据
				try {
					var lineRows = uncompressDataToMysqlData(line, yesterdayStr);
					mergeDbData(lineRows);
					readedLine++;
				}
				catch (e) {
					console.error(e.message);
				}
				if (last) {
					resolve(readedLine);
				}
			});
		});
		return promise;
	};
	var checkProess = function() {
		if (total == readed) {
			saveToDb()//打印入库结果
					.then(function(result) {
						console.info('[db] rows:%d', result.affectedRows);
						return Promise.resolve();
					})
					//退出进程
					.then(function() {
						console.info('process exit');
						process.exit(0);
					});
		}
		else {
			if (fileList.length > 0) {
				readFile().then(function(readedLine) {
					readed++;
					logger.console.info('[reader] Line:%d (%d/%d)', readedLine, readed, total);
					// console.log(dbData.length);
					checkProess();
				});
			}
		}
	};
	checkProess();
	checkProess();
	checkProess();
}
/**
 * 运行
 */
function run() {
	var argv = require('yargs').demand(['date']).default({date: 1}).describe({date: '前几天'}).argv;
	var moment = Moment();
	//获取昨天日期
	var yesterday = moment.subtract(argv.date, 'days');
	var yesterdayStr = yesterday.format('YYYY-MM-DD');
	var day = yesterday.format('DD');
	var month = yesterday.format('MM');
	var year = yesterday.format('YYYY');
	var keyPrefix = config.oss.prefix + '/' + year + '/' + month + '/' + day;
	logger.console.info('download ' + yesterdayStr + ' logs,use prefix: ' + keyPrefix);
	var downloadPath = __dirname + '/download';
	var currentDownloadPath = downloadPath + '/' + yesterdayStr;
	var logList = [];
	// setInterval(function(){
	// 	showMem();
	// }, 5000);
	//初始化下载目录
	return database().then(function(conn) {
		return conn.queryAsync('DELETE FROM ' + config.mysql.tablePrefix + 'logs WHERE `date`=?', [yesterdayStr]).then(function(resp) {
					logger.console.info('[mysql] remove ' + yesterdayStr + ' ' + resp.affectedRows + ' rows');
					conn.destroy();
				})
				.catch(function(e) {
					logger.console.error('[mysql] remove history rows fail:%s', e.message);
				})
				.then(function() {
					return unlinkAllExcludPath(downloadPath, yesterdayStr);
				})
				//创建目录
				.then(function() {
					return initPath(downloadPath).then(initPath(currentDownloadPath));
				})
				.then(function() {

					//已经处理的文件数
					var processed = 0, total = 0, readed = 0, downloaded = 0;
					var fileList = [];
					var emitter = oss.download(currentDownloadPath, {
						'max-keys': 1000,
						prefix: keyPrefix
					});
					emitter.on('listEmpty', function() {
						console.info('listEmpty triggered');
						process.exit(0);
					});
					emitter.on('nextDownload', function() {
						if (processed < total) {
							logger.console.info('[osslist] begin: %d ', processed);
							var object = logList[processed];
							processed++;
							emitter.emit('_download', object);
						}
					});
					emitter.on('listSuccess', function(result) {
						total = result.objects.length;
						logList = result.objects;
						// processed = 0;
						logger.console.info('[osslist] List Loaded Success: %d ', total);
						emitter.emit('nextDownload');
						emitter.emit('nextDownload');
						emitter.emit('nextDownload');
					});
					emitter.on('objectSuccess', function(objectName, localPath) {
						downloaded++;
						fileList.push(localPath);
						logger.console.trace('[downloader] %s %d/%d - %d%%', objectName, downloaded, total, parseInt(downloaded * 100 / total));
						if (downloaded == total) {
							//加载完成
							tongji(fileList, yesterdayStr);
							return;
						}
						else {
							emitter.emit('nextDownload');
						}
					});
					emitter.on('objectError', function(e, object) {
						logger.console.error('[processor] %d/%d - %d%%', downloaded, total, parseInt(readed * 100 / total));
						if (!object.tryed) {
							emitter.emit('_download', object);
						}
						else {
							downloaded++;
						}
					});
				});
	});
}
run();
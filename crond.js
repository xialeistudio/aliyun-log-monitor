/**
 * @author xialeistudio
 * @date 2016/5/18 0018
 */
'use strict';
var database = require('./lib/_database');
var logger = require('./lib/_logger');
var oss = require('./lib/_oss');
var snappy = require('./lib/_snappy');
var Moment = require('moment');
var Promise = require('bluebird');
var fs = require('fs');
var config = require('./config.json');
var _ = require('underscore');
Promise.promisifyAll(fs);
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
/**
 * 解压出来的数据转换为Mysql数组
 * @param data
 * @param date
 * @returns {Array}
 */
function uncompressDataToMysqlData(data, date) {
	var list = data.split('\n');
	var needData = [];
	list.forEach(function(line) {
		if (!line) {
			return;
		}
		try {
			var json = JSON.parse(line);
			var url = json.url;
			var rtime = parseFloat(json.rtime);
			//检测是否存在
			var isMatched = false, matchIndex = -1;
			needData.forEach(function(item, index) {
				if (item.url === url) {
					isMatched = true;
					matchIndex = index;
				}
			});
			var record;
			if (!isMatched) {
				record = {
					url: url,
					requestCount: 1,
					totalTime: rtime,
					averageTime: rtime,
					minTime: rtime,
					maxTime: rtime,
					date: date
				};
				needData.push(record);
			}
			else {
				record = needData[matchIndex];
				record.averageTime = parseFloat((record.averageTime * record.requestCount + rtime) / record.requestCount).toFixed(3);
				record.requestCount++;
				record.totalTime += rtime;
				record.minTime = Math.min(record.minTime, rtime);
				record.maxTime = Math.max(record.maxTime, rtime);
				needData[matchIndex] = record;
			}
		}
		catch (e) {
			logger.console.error('[json] parse %s error: %s', line, e.message);
		}
	});
	//mysql数据处理
	var mysqlData = [];
	needData.forEach(function(item) {
		var row = [item.url, item.requestCount, item.totalTime, item.averageTime, item.minTime, item.maxTime, item.date];
		mysqlData.push(row);
	});
	return mysqlData;
}
/**
 * 数据入库
 * @param mysqlData
 * @returns {*}
 */
function saveToDb(mysqlData) {
	var sql = 'INSERT INTO reg_logs (url,requestCount,totalTime,averageTime,minTime,maxTime,date) VALUES ?';
	//MySQL批量插入
	return database().then(function(conn) {
		return conn.queryAsync(sql, [mysqlData]).then(function(res) {
			conn.release();
			return res;
		}).catch(function(e) {
			conn.release();
			throw e;
		});
	});
}
/**
 * 运行
 */
function run() {
	var moment = Moment();
	//获取昨天日期
	var yesterday = moment.subtract(1, 'days');
	var yesterdayStr = yesterday.format('YYYY-MM-DD');
	var day = yesterday.format('DD');
	var month = yesterday.format('MM');
	var year = yesterday.format('YYYY');
	var keyPrefix = config.oss.prefix + '/' + year + '/' + month + '/' + day;
	logger.console.info('download ' + yesterdayStr + ' logs,use prefix: ' + keyPrefix);
	var downloadPath = __dirname + '/download';
	var snappyPath = downloadPath + '/' + yesterdayStr;
	//初始化下载目录
	initPath(downloadPath)
	//初始化子目录
			.then(function() {
				return initPath(snappyPath);
			})
			//下载snappy文件
			.then(function() {
				//下载
				var ee = oss.download(snappyPath, {
					'max-keys': 1000,
					'prefix': keyPrefix
				});
				var decoded = 0;
				ee.on('object', function(name, localPath, downloaded, total) {
					logger.console.trace('[oss] ' + downloaded + '/' + total + ' - ' + parseInt(downloaded * 100 / total) + '%');
					//开始解压
					snappy.decodeFile(localPath).then(function(data) {
						var mysqlData = uncompressDataToMysqlData(data, yesterdayStr);
						return saveToDb(mysqlData).then(function(res) {
							logger.console.info('[mysql] info: %s', JSON.stringify(res));
						});
					}).catch(function(e) {
						logger.console.error('[snappy] parse %s error: %s', localPath, e.message);
					}).then(function() {
						decoded++;
						logger.console.trace('[mysql] ' + decoded + '/' + total + ' - ' + parseInt(decoded * 100 / total) + '%');
					});
				});
				ee.on('end', function() {
					logger.console.trace('downloaded end');
				});
				ee.on('error', function(e) {
					logger.console.error('downloaded error: ' + e.message);
				});
			})
			.catch(function(e) {
				logger.console.error('error: ' + e.message);
			});
}
run();
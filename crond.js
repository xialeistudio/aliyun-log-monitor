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
var URL = require('url');
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
var dbData = [];//最终入库数据
//url,requestCount,totalTime,averageTime,minTime,maxTime,date
function mergeDbData(mysqlData) {
	if (dbData.length === 0) {
		dbData = dbData.concat(mysqlData);
	}
	else {
		dbData = dbData.map(function(dbItem) {
			var isMatched = false;
			mysqlData.forEach(function(mysqlItem) {
				if (mysqlItem.url == dbItem.url) {
					isMatched = true;
					//进行数据合并计算
					dbItem.requestCount += mysqlItem.requestCount;
					dbItem.totalTime += mysqlItem.totalTime;
					dbItem.averageTime = parseFloat((dbItem.totalTime + mysqlItem.totalTime) / dbItem.requestCount).toFixed(3);
					dbItem.minTime = Math.min(dbItem.minTime, mysqlItem.minTime);
					dbItem.maxTime = Math.max(dbItem.maxTime, mysqlItem.maxTime);
				}
			});
			return dbItem;
		});
	}
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
			var status = json.status;
			//检测是否存在
			var isMatched = false, matchIndex = -1;
			needData.forEach(function(item, index) {
				if (getUrlPattern(item.url) === getUrlPattern(url)) {
					isMatched = true;
					matchIndex = index;
				}
			});
			var record;
			if (!isMatched) {
				record = {
					url: getUrlPattern(url),
					requestCount: 1,
					totalTime: rtime,
					averageTime: rtime,
					minTime: rtime,
					maxTime: rtime,
					date: date,
					status: status
				};
				needData.push(record);
			}
			else {
				record = needData[matchIndex];
				record.requestCount++;
				record.totalTime += rtime;
				record.averageTime = parseFloat(record.totalTime / record.requestCount).toFixed(3);
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
		var row = [getUrlPattern(item.url), item.requestCount, item.totalTime, item.averageTime, item.minTime, item.maxTime, item.date, item.status];
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
	var sql = 'INSERT INTO ' + config.mysql.tablePrefix + 'logs (url,requestCount,totalTime,averageTime,minTime,maxTime,date,status) VALUES ?';
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
/**
 * 删除目录
 * @param path
 */
function unlinkPath(path) {
	var exec = require('child_process').exec, child;
	return new Promise(function(resolve, reject) {
		child = exec('rm -rf ' + path, function(err, out) {
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
 * 运行
 */
function run() {
	var moment = Moment();
	//获取昨天日期
	var yesterday = moment.subtract(1, 'days');
	// var yesterday = moment;
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
					'max-keys': 5,
					'prefix': keyPrefix
				});
				var decoded = 0;
				ee.on('list', function(list) {
					if (list.objects !== undefined) {
						logger.console.info('[object] should process ' + list.objects.length + ' files');
					}
					else {
						logger.console.info('[object] not file to process');
					}
				});
				ee.on('object', function(name, localPath, downloaded, total, objectName) {
					if (name === null) {
						decoded++;
						logger.console.trace('[mysql] ' + decoded + '/' + total + ' - ' + parseInt(decoded * 100 / total) + '%');
						if (decoded >= total) {
							process.exit(0);
						}
						return;
					}
					logger.console.trace('[oss] ' + downloaded + '/' + total + ' - ' + parseInt(downloaded * 100 / total) + '%');
					//开始解压
					fs.readFileAsync(localPath).then(function(data) {
								//删除oss
								return oss.remove(objectName).then(function() {
									logger.console.trace('[object] remove ' + objectName);
									return data;
								}).catch(function(e) {
									logger.console.error(e);
									return data;
								});
							})
							//处理为mysql数据
							.then(function(data) {
								var mysqlData = uncompressDataToMysqlData(data.toString(), yesterdayStr);
								//合并处理
								return mergeDbData(mysqlData);
							}).catch(function(e) {
						logger.console.error('[snappy] parse %s error: %s', localPath, e.message);
					}).then(function() {
						decoded++;
						logger.console.info('[mysql] ' + decoded + '/' + total + ' - ' + parseInt(decoded * 100 / total) + '%');
						if (decoded >= total) {
							//全部处理完毕，入库
							return saveToDb(dbData).then(function() {
								return 'ok';
							}).catch(function() {
								return 'error';
							}).then(function(result) {
								logger.console.info('[mysql] save: ' + result);
								return unlinkPath(snappyPath)
										.then(function() {
											logger.console.info('[clean] ' + snappyPath + ' success');
											process.exit(0);
										}).catch(function(e) {
											logger.console.error('[clean] ' + snappyPath + ' error: ' + e.message);
											process.exit(0);
										});
							});
						}
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
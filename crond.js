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
/**
 * 数据入库
 * @param mysqlData
 * @returns {Array}
 */
function mergeDbData(mysqlData) {
	if (dbData.length === 0) {
		dbData = dbData.concat(mysqlData);
	}
	else {
		//检测mysqldata中的URL是否在dbdata中
		mysqlData.forEach(function(mysqlItem) {
			var isMatched = false;
			dbData.forEach(function(dbItem, index) {
				if (dbItem[0] == mysqlItem[0]) {
					isMatched = true;
					//合并数据
					dbItem[1] += mysqlItem[1];
					dbItem[2] += mysqlItem[2];
					dbItem[3] = parseFloat((dbItem[2] / dbItem[1]).toFixed(3));
					dbItem[4] = Math.min(dbItem[4], mysqlItem[4]);
					dbItem[5] = Math.max(dbItem[5], mysqlItem[5]);
					dbData[index] = dbItem;
				}
				if (!isMatched) {
					dbData.push(mysqlItem);
				}
			})
		});
	}
	return dbData;
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
		if (line.length === 0) {
			return;
		}
		try {
			var json = JSON.parse(line);
			var url = json.url;
			var rtime = parseFloat(json.rtime);
			var status = parseInt(json.status);
			//检测是否存在
			var isMatched = false, matchIndex = -1;
			needData.forEach(function(item, index) {
				if (item.url === getUrlPattern(url)) {
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
 * @returns {*}
 */
function saveToDb() {
	var sql = 'INSERT INTO ' + config.mysql.tablePrefix + 'logs (url,requestCount,totalTime,averageTime,minTime,maxTime,date,status) VALUES ?';
	//MySQL批量插入
	return database().then(function(conn) {
		return conn.queryAsync(sql, [dbData]).then(function(res) {
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
	var currentDownloadPath = downloadPath + '/' + yesterdayStr;
	//初始化下载目录
	return database().then(function(conn) {
		return conn.queryAsync('DELETE FROM ' + config.mysql.tablePrefix + 'logs WHERE `date`=?', [yesterdayStr]).then(function(resp) {
					logger.console.info('[mysql] remove ' + yesterdayStr + ' ' + resp.affectedRows + ' rows');
					conn.release();
				})
				.catch(function(e) {
					logger.console.error('[mysql] remove history rows fail:%s', e.message);
				})
				//创建目录
				.then(function() {
					return initPath(downloadPath).then(initPath(currentDownloadPath));
				})
				.then(function() {
					var emitter = oss.download(currentDownloadPath, {
						'max-keys': 3,
						prefix: keyPrefix
					});
					//已经处理的文件数
					var processed = 0, total = 0;
					emitter.on('listEmpty', function() {
						console.info('listEmpty triggered');
						process.exit(0);
					});
					emitter.on('listSuccess', function(result) {
						total = result.objects.length;
					});
					emitter.on('objectSuccess', function(objectName, localPath, downloaded, total) {
						//读取文件，逐行
						fs.readFileAsync(localPath)
								//文件数据
								.then(function(buffer) {
									return Promise.resolve(buffer.toString());
								})
								//转化为mysql数组
								.then(function(data) {
									return Promise.resolve(uncompressDataToMysqlData(data, yesterdayStr));
								})
								//合并历史数据
								.then(function(rows) {
									return Promise.resolve(mergeDbData(rows));
								})
								//检测完成
								.then(function() {
									processed++;
									return Promise.resolve(processed === total);
								})
								//入库
								.then(function(isCompleted) {
									if (isCompleted) {
										return saveToDb()//打印入库结果
												.then(function(result) {
													console.info('[db] ' + JSON.stringify(result));
													return Promise.resolve();
												})
												//删除下载目录
												.then(function() {
													return unlinkPath(currentDownloadPath);
												})
												//退出进程
												.then(function() {
													console.info('process exit');
													process.exit(0);
												});
									}
								})
					});
					emitter.on('objectError', function(e, downloaded, total) {
						processed++;
						if (processed === total) {
							//入库
							saveToDb()
							//打印入库结果
									.then(function(result) {
										console.info('[db] ' + JSON.stringify(result));
										return Promise.resolve();
									})
									//删除下载目录
									.then(function() {
										return unlinkPath(currentDownloadPath);
									})
									//退出进程
									.then(function() {
										console.info('process exit');
										process.exit(0);
									});
						}
					});
				});
	});
}
run();
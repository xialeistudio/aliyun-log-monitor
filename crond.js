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
		dbData.push(mysqlData);
	}
	else {
		//检测mysqldata中的URL是否在dbdata中
		var isMatched = false;
		dbData.forEach(function(dbItem, index) {
			if (dbItem[0] == mysqlData[0]) {
				//合并数据
				isMatched = true;
				dbItem[1] += mysqlData[1];
				dbItem[2] += mysqlData[2];
				dbItem[3] = parseFloat((dbItem[2] / dbItem[1]).toFixed(3));
				dbItem[4] = Math.min(dbItem[4], mysqlData[4]);
				dbItem[5] = Math.max(dbItem[5], mysqlData[5]);
				dbData[index] = dbItem;
			}
		});
		if (!isMatched) {
			dbData.push(mysqlData);
		}
	}
	return dbData;
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
 * 内存情况
 */
var showMem = function() {
	var mem = process.memoryUsage();
	var format = function(bytes) {
		return (bytes / 1024 / 1024).toFixed(2) + 'MB';
	};
	console.log('[memory] heapTotal: %s heapUsed: %s rss %s', format(mem.heapTotal), format(mem.heapUsed), format(mem.rss));
};
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
	//初始化下载目录
	return database().then(function(conn) {
		return conn.queryAsync('DELETE FROM ' + config.mysql.tablePrefix + 'logs WHERE `date`=?', [yesterdayStr]).then(function(resp) {
			logger.console.info('[mysql] remove ' + yesterdayStr + ' ' + resp.affectedRows + ' rows');
			conn.destroy();
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
						'max-keys': 1000,
						prefix: keyPrefix
					});
					//已经处理的文件数
					var processed = 0, total = 0, readed = 0;
					emitter.on('listEmpty', function() {
						console.info('listEmpty triggered');
						process.exit(0);
					});
					emitter.on('listSuccess', function(result) {
						total = result.objects.length;
					});
					emitter.on('objectSuccess', function(objectName, localPath, downloaded, total) {
						logger.console.trace('[downloader] %d/%d - %d%%', downloaded, total, parseInt(downloaded * 100 / total));
						//读取文件，逐行
						var promise = new Promise(function(resolve, reject) {
							lineReader.eachLine(localPath, function(line, last) {
								//处理行数据
								try {
									var lineRows = uncompressDataToMysqlData(line, yesterdayStr);
									mergeDbData(lineRows);
								}
								catch (e) {
									console.error(e.message);
								}
								if (last) {
									readed++;
									logger.console.info('[reader] %d/%d - %d%%', readed, total, parseInt(readed * 100 / total));
									resolve();
								}
							});
						});
						promise
						//检测完成
								.then(function() {
									showMem();
									processed++;
									logger.console.warn('[processor] %d/%d - %d%%', readed, total, parseInt(readed * 100 / total));
									return Promise.resolve(processed === total);
								})
								//入库
								.then(function(isCompleted) {
									if (isCompleted) {
										return saveToDb()//打印入库结果
												.then(function(result) {
													console.info('[db] rows:%d', result.affectedRows);
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
						showMem();
						processed++;
						logger.console.warn('[processor] %d/%d - %d%%', readed, total, parseInt(readed * 100 / total));
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
										process.exit(0);
									});
						}
					});
				});
	});
}
run();
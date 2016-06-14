/**
 * @author xialeistudio
 * @date 2016/5/18 0018
 */
'use strict';
var log4js = require('log4js');
var fs = require('fs');
//创建目录
var logPath = __dirname + '/../logs';
if (!fs.existsSync(logPath)) {
	fs.mkdirSync(logPath, 666);
}
//加载配置
log4js.configure(require('../log4js.json'));
module.exports = {
	console: log4js.getLogger('console'),
	file: log4js.getLogger('file')
};
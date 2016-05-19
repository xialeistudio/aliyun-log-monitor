/**
 * @author xialeistudio
 * @date 2016/5/18 0018
 */
'use strict';
var Promise = require('bluebird');
var mysql = require('mysql');
var Pool = require('mysql/lib/Pool');
var Connection = require('mysql/lib/Connection');
var logger = require('./_logger');
Promise.promisifyAll(Pool);
Promise.promisifyAll(Pool.prototype);
Promise.promisifyAll(Connection);
Promise.promisifyAll(Connection.prototype);
var config = require('../config.json').mysql;
var pool = mysql.createPool(config);
pool.on('connection', function(connection) {
	logger.console.trace('[Mysql] connection ' + connection.threadId + ' initialized');
});
module.exports = function() {
	return pool.getConnectionAsync();
};
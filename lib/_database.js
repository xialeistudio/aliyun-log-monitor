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
Promise.promisifyAll(Connection);
var config = require('../config.json').mysql;
var pool = mysql.createPool(config);
pool.on('connection', function(connection) {
	logger.console.info('[Mysql] connection ' + connection.threadId + ' initialized');
});
pool.on('enqueue', function() {
	logger.console.warn('[Mysql] wait for connection');
});
module.exports = function() {
	return pool.getConnectionAsync();
};
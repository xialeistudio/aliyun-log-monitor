/**
 * @author xialeistudio
 * @date 2016/5/18 0018
 */
'use strict';
require('should');
var oss = require('../lib/_oss');
var config = require('../config.json').oss;
var path = __dirname + '/../download/2016-05-17';
describe('test oss', function() {
	it('test download', function(done) {
		var ee = oss.download(path, {
			'max-keys': 5,
			'prefix': config.prefix + '/2016/05/17'
		});
		ee.on('object', function(name, path) {
			console.log(name + ' ==> ' + path);
		});
		ee.on('error', done);
		ee.on('end', function() {
			done();
		});
	});
});
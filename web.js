/**
 * Created by rices on 16/6/15.
 */
var http = require("http");
var url = require("url");
var fs = require("fs");
var Promise = require('bluebird');
Promise.promisifyAll(fs);
var moment = require("moment");

var getRealUrl = function(hash) {
	return pathMap[hash] ? pathMap[hash] : hash;
};

function showLog(ip){
	var file = fs.readFileSync('./path/'+ip.replace(/\./g, '-')+'.txt', 'utf8');
	var html = '';
	var lines = file.split('\n');
	console.dir(lines);
	var last = 0;
	for(var i in lines){
		if(lines[i]){
			var lineArr = lines[i].split('|');
			if(lineArr[1] - last > 600){
				html += "<p>===================</p>";
			}
			else if(lineArr[1] - last > 3){
				html += "<p><hr></p>";
			}
			last = lineArr[1];
			var date = new Date(lineArr[1]*1000);
			html += "<p>"+getRealUrl(lineArr[0])+'  | '+moment(lineArr[1]*1000).format('YYYY-MM-DD HH:mm:ss');+"</p>";
		}

	}
	return html;
}
var mapData = fs.readFileSync('./path/map.txt', 'utf8');
var pathMap = JSON.parse(mapData);

http.createServer(function(request, response) {
	var params = url.parse(request.url, true).query;
	var html = '';
	if(params.ip){
		html = showLog(params.ip);
	}
	response.writeHead(200, {"Content-Type": "text/html"});
	response.write(html);
	response.end();

}).listen(3005);

'use strict';
var fs = require('fs');
var path = require('path');
var lineReader = require('line-reader');
var _ = require('underscore');
var fileList = [];
var readed = 0;
var total = 0;
var result = {};
var pathMap = {};
var maxLevel = 5;
var levelNodeLimit = 30;
//初始化等级
for (var _level = 0; _level <= maxLevel; _level++) {
	result["" + _level] = [];
}
//读取文件，逐行
var readFile = function() {
	var promise = new Promise(function(resolve, reject) {
		var filePath = fileList.shift();
		var readedLine = 0;
		var level = 0;
		var lastDate = 0;
		if (filePath.indexOf('map.txt') != -1) {
			resolve(0, 0);
			return;
		}
		try {
			var form = undefined;
			var to = '';
			lineReader.eachLine(filePath, function(line, last) {
				//处理行数据
				try {
					var lineArr = line.split('|');
					//开始计算入口单元
					if (form == undefined || lineArr[1] - lastDate > 600) {
						form = lineArr[0];
						to = lineArr[0];
						level = 0;
						lastDate = lineArr[1];
					}
					else {
						form = to;
						to = lineArr[0];
					}

					if (form != to) {
						//最大分析级数限制
						if (level <= maxLevel) {

							//冷却时间10秒 如果5秒内连续请求 (这样连线不对!!!)
							if(lineArr[1] - lastDate > 3){
								var k = form + '-' + to;
								if (!result["" + level][k]) {
									result["" + level][k] = 0;
								}
								result["" + level][k]++;

								level++;
							}

							lastDate = lineArr[1];
						}
					}
					// console.dir(lineArr);
					readedLine++;
				}
				catch (e) {
					console.error(filePath + e.message);
				}
				if (last) {
					// console.error(filePath+'---LAST');
					resolve(readedLine, level);
				}
			});
		}
		catch (e) {
			resolve(readedLine, level);
			console.error(filePath + e.message);
		}
	});
	return promise;
};
var outputLinks = [];
var outputNodes = [];
var nodesMap = {};

var signatureMap = {
	'/v1/app/config': 'APP首页',
	'/v1/oauth/loginbywechat': '微信登陆',
	'/v1/app/setpush': 'APP登录',
	'/v1/phonebook/createlist': '首页',
	'/v1/phonebook/tops': '首页',
	'/v1/phonebookcommunity/messages': '通讯录社区',
	'/v1/phonebook/activemembers': '通讯录首页',
	'/v1/service/list': '服务',
	'/v1/channel/systemlist': '系统消息',
	'/v1/chat/*': '消息页',
	'/v1/channel/chatlist': '消息列表',
	'/v1/usermoneylog/income': '钱包',
	'/v1/withdrawal/pending': '钱包',
	'/v1/friend/applylist': '好友申请',
	'/v1/user/simple': '看名片',
	'/v1/card/hit': '看名片',
	'/phonebook/*/s/*': '通讯录(引荐)',
	'/oauth/login': '登录',
	'/app/message': '系统消息',
	'/v1/service/usercreated': '创建服务'
};
var getSignature = function() {

	_.mapObject(signatureMap, function(val, key) {

	});

}

var getRealUrl = function(hash) {
	return pathMap[hash] ? pathMap[hash] : hash;
};
var addNode = function(nodeName, url) {

	var col = 0;
	if(url.indexOf('phonebook') != -1 || url.indexOf('/b/') != -1){
		col = 1;
	}else if(url.indexOf('activity') != -1){
		col = 2;
	}else if(url.indexOf('card') != -1){
		col = 3;
	}else if(url.indexOf('message') != -1){
		col = 4;
	}else if(url.indexOf('chat') != -1){
		col = 5;
	}else if(url.indexOf('app') != -1){
		col = 6;
	}


	var nodeId = 0;
	if (!nodesMap[nodeName]) {
		nodeId = outputNodes.length;
		outputNodes.push({
			name: nodeName,
			type: col,
			title: url
		});
		nodesMap[nodeName] = nodeId;
	}
	else {
		nodeId = nodesMap[nodeName];
	}
	return nodeId;
}
var outPutResult = function() {

	//加工数组
	//读取列表
	var mapData = fs.readFileSync(baskPath + '/map.txt', 'utf8');
	pathMap = JSON.parse(mapData);
	_.mapObject(result, function(levelResult, level) {
		level = parseInt(level);
		_.mapObject(levelResult, function(value, key) {
			if ((level > 0 && value > 200)|| (level == 0 && value > 500)) {
				// console.dir(value);
				var keyArr = key.split('-');
				var formNodeName = level + '' + keyArr[0];
				var toNodeName = (level + 1) + '' + keyArr[1];
				if (level == 0 || nodesMap[formNodeName]) {
					var formNodeId = addNode(formNodeName, getRealUrl(keyArr[0]));
					var toNodeId = addNode(toNodeName, getRealUrl(keyArr[1]));
					outputLinks.push({
						source: formNodeId,
						target: toNodeId,
						value: value
					});
				}
			}
		});
	});
	console.log('Output: nodes:%d links: %d', outputNodes.length, outputLinks.length);
	//生产JSON
	// fs.unlinkSync('./result.json');
	fs.writeFile('./result.json', JSON.stringify({
		nodes: outputNodes,
		links: outputLinks
	}));
};
var fileRun = function() {
	if (total == readed) {
		outPutResult();
	}
	else {
		if (fileList.length > 0) {
			readFile().then(function(readedLine, level) {
				readed++;
				console.log('[reader] Line: %d - %d (%d/%d)', readedLine, level, readed, total);
				fileRun();
			}).catch(function(e) {
				readed++;
				console.log('[reader] Line:' + e.message);
				fileRun();
			});
		}
		else {
			console.log('over');
		}
	}
};
function ls(ff) {
	var files = fs.readdirSync(ff);
	for (var fn in files) {
		var fname = ff + path.sep + files[fn];
		var stat = fs.lstatSync(fname);
		if (stat.isFile() == true) {
			fileList.push(fname);
		}
	}
	total = fileList.length;
	readed = 0;
	fileRun();
}
var baskPath = __dirname + '/path';
ls(baskPath);

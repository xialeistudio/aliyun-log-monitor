阿里云日志服务web终端
将接入阿里云的Ngin/Apache日志显示在web上
#目录结构
+ download 下载目录
+ lib 库文件目录
+ test 测试目录
+ config.json 配置文件（请自行创建） 
+ index.js 入口文件
+ log4js.json log4j配置文件
+ package.json meta文件
+ README.md 描述文件
## config.json格式
		{
		  "oss": {
		    "region": "OSS region",
		    "accessKeyID": "OSS accessKeyID",
		    "accessKeySecret": "OSS accessKeySecret",
		    "bucket": "日志服务存储Bucket",
		    "prefix": "key前缀，如log (程序自动拼接日期)"
		  },
		  "server": {
		    "port": http服务器监听端口
		  },
		  "mysql": {
		    "host": "mysql主机",
		    "port": mysql端口,
		    "poolSize": 连接池大小,
		    "database": "数据库名称",
		    "username": "数据库账号",
		    "password": "数据库密码",
		    "tablePrefix": "表前缀（用来去不不同项目）"
		  }
		}
#运行
		npm install
		npm run start
#单元测试
		npm install -g mocha
		mocha
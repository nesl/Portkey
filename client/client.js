var fs = require('fs')
var RedisClustr = require('./redis-clustr')

console.log("USAGE: node client.js HostIP Port CommandFile.txt [IGNORE]")

var commands = process.argv[4]
var redisHost = process.argv[2]
var redisPort = process.argv[3] ? process.argv[3] : '6379'
var redis = new RedisClustr({servers:[{host: redisHost, port: redisPort}]});

var ignore = process.argv[5]

function paretoDistribution (minimum, alpha) {
    var u = 1.0 - Math.random();
    return minimum / Math.pow(u, 1.0 / alpha);
}

var executeCommandsRecursive = async function(cmds, callback) {

	if(cmds.length == 0) {
		return callback(null);
	}

	var res = cmds.shift().split(",");
	var cmdType = res[0]
	var key = res[1]
	if (!res[0] || !res[1]) {
		return callback(cmdType + key + "ERROR CANNOT PARSE: ");
	}
	console.log(cmds.length + ": " + cmdType + "  \t" + key + "\t" + (new Date()).toLocaleTimeString());
	if(cmdType === "update") {
		var randomfield = "field" + Math.floor(Math.random() * 10);
		var randomChar = Math.random().toString(36).substring(2,3);
		var value = {};
		value[randomfield] = new Array(101).join(randomChar);
		redis.hmset(key, value, function(err, resp) {
			if (err) callback(cmdType + key + err);
			executeCommandsRecursive(cmds, callback);
		})
	} else if (cmdType === "read") {
		redis.hgetall(key, function(err, object) {
			if (err) callback(cmdType + key + err);
			executeCommandsRecursive(cmds, callback);
		});
	} else if (cmdType === "insert") {
		var value = {}
		for(var i=0; i<10; i++) {
			var field = "field" + i
			var randomChar = Math.random().toString(36).substring(2,3);
			var fieldValue = new Array(101).join(randomChar);
			value[field] = fieldValue;
		}
		redis.hmset(key, value, function(err, resp) {
			if (err) callback(cmdType + key + err);
			executeCommandsRecursive(cmds, callback);
		});
	}
}

redis.setSketchingEnabled(!ignore);

fs.readFile(commands, 'utf8', function(err, contents) {
	var allCMDs = contents.split('\n');

	executeCommandsRecursive(allCMDs, function(err) {
		if (err) console.log("Error running trace", err);
		else console.log("done!");

		redis.printLog();
		redis.storeSketch(function(err) {
                    	if(err) console.log("ERROR storing sketch:", err);
                    	redis.quit();
            	});
	});
});

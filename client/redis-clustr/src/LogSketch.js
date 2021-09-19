var calculateSlot = require('cluster-key-slot');

var sketch = {slots:{}};
var profile = {};
var ordering = [];
var lastStore = undefined;
var redis = undefined;
var addedClientToRedis = false;

const { DDSketch } = require('sketches-js');

var optimizationMetric = 'avg'

var enabled = true;
module.exports.setEnabled = function(enble) {
  enabled = enble;
}

module.exports.printLog = function() {
  var totalsum = 0;
  var totalcnt = 0;
  Object.keys(sketch).forEach(function(node) {
    var cnt = sketch[node].cnt
    var latency = getRequestLatency(node);
    console.log("Accessed", node, cnt, "times. Took", latency, "ms...");
    if(latency && cnt && cnt > 2) {
      totalsum += latency * cnt;
      totalcnt += cnt;
    }
  });
  console.log("Accessed ", Object.keys(sketch.slots).length, " slots");//, Object.keys(sketch.slots));
  console.log("Overview: total of ", totalsum, "ms with", totalcnt, "requests");
  console.log("Overview: avg latency:", totalsum/totalcnt, "ms");
};

var getStorableSketch = function() {
  var storableSketch = {slots:{}}
  Object.keys(sketch).forEach(function(node) {
    if(node == 'time' || node == 'slots') {
      storableSketch[node] = sketch[node]
    } else {
      storableSketch[node] = {
        cnt: sketch[node].cnt,
        sum: getRequestLatency(node) * sketch[node].cnt
      }
      profile[node] = storableSketch[node].sum / storableSketch[node].cnt;
    }
  });
  return JSON.stringify(storableSketch);
}

var activeProbe = function() {
  var masters = new Set();
  var pingcmds = []
  Object.keys(redis.connections).forEach(function(node) {
    if(redis.connections[node].master) {
      masters.add(node);
    }
  });

  var local = Object.entries(profile).sort((node1, node2) => node1[1] < node2[1]).slice(0, 4).map((node) => node[0]);
  var trigger = local.length === ordering.length && local.every(function(value, index) { return value === ordering[index]});
  if(!trigger) masters = new Set(local);
  for (let node of masters) {
    if (sketch[node]) masters.delete(node);
  }

  var i = 0;
  while(masters.size > 0) {
    var key = i.toString();
    i+=1;
    var slot = calculateSlot(key);
    var server = redis.slots[slot][0].address;

    if(masters.has(server)) {
      redis.hgetall(key, function(err, object) {});
      masters.delete(server);
    }
  }
  return pingcmds;
}

var clientID = require('uuid/v1')();
module.exports.storeSketch = function(callback) {
  if (!enabled) return callback("Sketching disabled...");
  sketch['time'] = Date.now()
  activeProbe();
  redis.setex(clientID, 6000, getStorableSketch(), function(err, resp) {
    if (err) return callback(err);
    else {
      console.log("set sketch into redis.");
      lastStore = process.hrtime();
      sketch = {slots:{}};

      //ensure client is stored in the client-key
      if (!addedClientToRedis) {
        redis.sadd("allclients", clientID, function(err, resp) {
          if (err) return callback(err);
          else addedClientToRedis = true;
          return callback(null);
        });
      } else {
        return callback(null);
      }
    }
  });
}

var logLatency = function(host, elapsed_time) {
  if (optimizationMetric === 'avg') {
    if (sketch[host] === undefined) {
      sketch[host] = {
        cnt: 0,
        sum: 0.0,
      }
    }
    sketch[host].cnt += 1
    sketch[host].sum += elapsed_time
  } else {
    if(sketch[host] === undefined) {
      sketch[host] = {
        cnt: 0,
        ddsketch: new DDSketch({alpha: 0.02})
      }
    }
    sketch[host].cnt += 1
    sketch[host].ddsketch.add(elapsed_time)
  }
}

var getRequestLatency = function(host) {
  if(optimizationMetric === 'avg' || sketch[host].ddsketch === undefined) {
    return sketch[host].sum / sketch[host].cnt;
  } else {
    return sketch[host].ddsketch.quantile(0.95);
  }
}

module.exports.logRequest = function(cli, cmd, key, elapsed, RedisClustr) {
  redis = RedisClustr
  if (cmd != "set" && cmd != "get" && cmd != "hmset" && cmd != "hgetall") return;

  var slot = calculateSlot(key);
  sketch.slots[slot] = (sketch.slots[slot] || 0) + 1;

  logLatency(cli.address, elapsed[0]*1000 + elapsed[1] / 1000000);

  if (!lastStore) lastStore = process.hrtime();
  var sketchDumpPeriod = 10
  if (enabled && process.hrtime(lastStore)[0] > sketchDumpPeriod) {
    module.exports.storeSketch(function(err) {
      if(err) console.log("ERROR", err);
    })
  }
};

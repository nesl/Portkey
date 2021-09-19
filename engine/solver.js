const fs = require('fs');
var redisHost = process.argv[2] ? process.argv[2] : '172.17.40.64'
var redisPort = process.argv[3] ? process.argv[3] : '7000'

var math = require('mathjs');
var RedisClustr = require('redis-clustr');
var redis = new RedisClustr({servers:[{host: redisHost, port: redisPort}]});
var migrate = require('./migrate.js');

var sketches = {}

var clients = []
var servers = []
var slots = []
var num_sketches_loaded = 0;

var SlotAccess = {}
var NetworkOracle = []
var CostMatrix = undefined;

var printDoubleArray = function(matrix) {
	matrix.forEach(function(arr) {
		var string = "[ ";
		arr.forEach(function(val) {
			string += val.toFixed(2) + "\t";
		});
		string += "]";
	})
}

var DumpMigrationToFile = function(currentSlotAssignment, slotAssignments, callback) {

	var dump = []

	var nodeToID = {}
	for (var i=0; i<currentSlotAssignment.length; i++) {
		var master = currentSlotAssignment[i][2];
		var masterAddress = master[0] + ":" + master[1];
		var masterID = master[2];
		nodeToID[masterAddress] = masterID;
	}

	for (var slotAssignmentIndex = 0; slotAssignmentIndex < slotAssignments.length; slotAssignmentIndex++) {
		var slot_to_assign = slotAssignments[slotAssignmentIndex];
		var slot = slot_to_assign[0]
		for (var i=0; i<currentSlotAssignment.length; i++) {
			var start = currentSlotAssignment[i][0]
			var end = currentSlotAssignment[i][1]

			var master = currentSlotAssignment[i][2]

			if (start <= slot && slot <= end) {
				var srcNodeID = master[2];
				var destNodeID = nodeToID[slot_to_assign[2]];

				if(srcNodeID != destNodeID) {
					dump.push([slot, srcNodeID, destNodeID])
				}
			}
		}
	}

	dump.sort(function(a,b) {
		if(a[2] != b[2]) {
			return (a[2] < b[2]) ? -1 : 1;
		} else if(a[1] != b[1]) {
			return a[0]-b[0];
		} else {
			return a[0]-b[0];
		}
	});

	var dumpString = "";
	for(var i=0; i<dump.length; i++) {
		dumpString += dump[i][0] + " " + dump[i][1] + " " + dump[i][2] + "\n";
	}

	fs.writeFile("./migrate_these_slots.txt", dumpString, function(err) {
		callback(err);
	});
}

var PlacementSolver = function(CostMatrix, slotAssignments, servers) {
	console.log("===========SOLVING============");
	printDoubleArray(CostMatrix);

	for(var slotIndex = 0; slotIndex < slotAssignments.length; slotIndex++) {
		var costVector = CostMatrix[slotIndex];
		var indexOfMinValue = costVector.reduce((iMax, x, i, arr) => x < arr[iMax] ? i : iMax, 0);
		slotAssignments[slotIndex].push(servers[indexOfMinValue]);
	}

	redis.cluster("SLOTS", function(err, currentSlotAssignment) {
		if(err) return callback("Failed to get slots...");

		DumpMigrationToFile(currentSlotAssignment, slotAssignments, function(err) {
			if(err) console.log(err);
			redis.quit();
		});

		// alternative: migrate.js
		// migrate.migrateSlots(currentSlotAssignment, slotAssignments, function(err) {
		// 	if (err) console.log(err);
		// 	redis.quit();
		// })
	});

}

redis.smembers("allclients", function(err, allclients) {
	if(err) return console.log(err);

	console.log("clients", allclients);
	if(allclients.length == 0) {
		redis.quit();
		return;
	}
	console.log("===========QUERYING===========");

	num_sketches_loaded = 0;
	var activeclients = []
	var activeclientSketches = []

	allclients.forEach(function(client, clientindex) {
		redis.get(client, function(err, resp) {
			if (err) console.log(err);
			else {
				num_sketches_loaded+= 1;
				if (!resp) {
					console.log("Removing", client, "...");
					redis.srem("allclients", client, function(err, resp) {
						if (err) console.log(err);
						else console.log("Removed", client);
					});
				} else {
					var parseSketch = JSON.parse(resp);
					activeclients.push([client, parseSketch]);
				}
				if (num_sketches_loaded == allclients.length) {

					console.log("===========EXTRACT============");
					clients = activeclients;
					if (clients.length == 0) {
						redis.quit();
						return;
					}

					activeclients.forEach(function(active_client_sketch, active_client_index) {
						var clientSketch = active_client_sketch[1];

						Object.keys(clientSketch.slots).forEach(function(slot) {
							if (!(slot in SlotAccess)) {
								SlotAccess[slot] = Array(activeclients.length).fill(0);
							}
							SlotAccess[slot][active_client_index] = clientSketch.slots[slot];
						});

						Object.keys(clientSketch).forEach(function(server) {
							if (server != "slots" && server != "time") {
								var client_server_sketch = clientSketch[server];
								
								var client_server_distance = client_server_sketch.sum / client_server_sketch.cnt;

								var serverindex = servers.indexOf(server);
								if (serverindex == -1) {
									servers.push(server);
									serverindex = servers.indexOf(server);
								}

								while (active_client_index >= NetworkOracle.length) {
									NetworkOracle.push([]);
								}
								while (serverindex >= NetworkOracle[active_client_index].length) {
									NetworkOracle[active_client_index].push(9999999);
								}
								NetworkOracle[active_client_index][serverindex] = client_server_distance
							}	
						});
					});

					console.log("===========BUFFERING==========");
					var num_servers = 0;
					for(var ci = 0; ci < NetworkOracle.length; ci++) {
						num_servers = Math.max(num_servers, NetworkOracle[ci].length);
					}
					for(var ci = 0; ci < NetworkOracle.length; ci++) {
						var maxValueForThisClient = Math.max(...NetworkOracle[ci]);
						while(NetworkOracle[ci].length < num_servers) {
							NetworkOracle[ci].push(maxValueForThisClient);
						}
					}

					console.log("===========PROCESS============");
					var SlotAccessDoubleArray = [];
					Object.keys(SlotAccess).forEach(function(slot, slotIndex) {
						//Normalize the slot access
						const total = SlotAccess[slot].reduce((a,b) => a+b, 0);
						slots.push([slot, total]);
						const normalized = SlotAccess[slot].map(x => x * 1.0 / total);
						SlotAccessDoubleArray.push(normalized);
					});

					var SlotAccessMatrix = math.matrix(SlotAccessDoubleArray);
					var NetworkOracleMatrix = math.matrix(NetworkOracle);

					CostMatrix = math.multiply(SlotAccessMatrix, NetworkOracleMatrix);

					var CostMatrixDoubleArray = []
					for (var i = 0; i < CostMatrix.size()[0]; i++) {
						CostMatrixDoubleArray.push(Array(CostMatrix.size()[1]).fill(0))
					}
					CostMatrix.forEach(function(value, index, matrix) {
						CostMatrixDoubleArray[index[0]][index[1]] = value;
					});
					
					PlacementSolver(CostMatrixDoubleArray, slots, servers);
				}
			}
		});
	});
});
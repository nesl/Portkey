var redis = require("redis");

var redisMasters = new Set();
var redisClients = {}
var getClient = function(client) {
    if (!(client in redisClients)) {
        var hostPort = client.split(":")
        redisClients[client] = redis.createClient({host: hostPort[0], port: hostPort[1]});
    }
    return redisClients[client];
}
var clearClients = function() {
    Object.keys(redisClients).forEach(function(indexingKey) {
        redisClients[indexingKey].quit();
        delete redisClients[indexingKey];
    });
}

module.exports.migrateSlots = function(currentSlotAssignment, newSlotAssignments, callback, errors = []) {
    if (newSlotAssignments.length == 0) {
        clearClients();
        return callback(errors.length > 0 ? errors : null);
    }

    for (var i=0; i<currentSlotAssignment.length; i++) {
        var master = currentSlotAssignment[i][2];
        redisMasters.add(master[0] + ":" + master[1]);
    }

    var slot_to_assign = newSlotAssignments.shift();

    migrateSlot(slot_to_assign[0], slot_to_assign[2], currentSlotAssignment, function(err) {
        if (err) errors.push(err);
        module.exports.migrateSlots(currentSlotAssignment, newSlotAssignments, callback, errors);
    });
}

var migrateSlot = function(keySlot, dest, currentSlotAssignment, callback) {
    var destNodeID = undefined
    var srcNodeID = undefined

    var srcClient = undefined;
    var destClient = undefined;

    var destHost = dest.split(":")[0]
    var destPort = dest.split(":")[1]

    keySlot = Number(keySlot);

    for (var i=0; i<currentSlotAssignment.length; i++) {
        var start = currentSlotAssignment[i][0]
        var end = currentSlotAssignment[i][1]

        var master = currentSlotAssignment[i][2]

        if (!destNodeID && master[0] == destHost && master[1] == destPort) {
            destNodeID = master[2];
            destClient = getClient(master[0] + ":" + master[1]);
        }
        if (start <= keySlot && keySlot <= end) {
            srcNodeID = master[2];
            srcClient = getClient(master[0] + ":" + master[1]);
        }
    }
    if (!destNodeID || !srcNodeID) {
        return callback(keySlot + ": cant find node." + destNodeID + " " + srcNodeID + " Exiting...");
    } else if (destNodeID == srcNodeID) {
        return callback();
    }

    destClient.cluster("SETSLOT", keySlot, "IMPORTING", srcNodeID, function(err, ok) {
        if(err) return callback(keySlot + ": failed to set dest importing..." + err);
        srcClient.cluster("SETSLOT", keySlot, "MIGRATING", destNodeID, function(err, ok) {
            if(err) return callback(keySlot + ": failed to set src migrating..." + err);
            srcClient.cluster("GETKEYSINSLOT", keySlot, 20, function(err, keysToMigrate) {
                if(err) return callback(keySlot + ": failed to get keys to migrate..." + err);
                var migrateArgs = [destHost, destPort, "", 0, 5000, "KEYS"]
                var migrateKeysRecursive = function(keysToMigrate, noMoreKeysCallback) {
                    if(keysToMigrate.length == 0) return noMoreKeysCallback();
                    srcClient.migrate(migrateArgs.concat(keysToMigrate), function(err, resp) {
                        if(err) return noMoreKeysCallback(keySlot + ": failed to migrate..." + err);
                        srcClient.cluster("GETKEYSINSLOT", keySlot, 20, function(err, KTM) {
                            if(err) return noMoreKeysCallback(keySlot + ": failed to get keys to migrate..." + err);
                            if(KTM.length > 0) return migrateKeysRecursive(KTM, noMoreKeysCallback);
                            else return noMoreKeysCallback();
                        })
                    })
                }
                migrateKeysRecursive(keysToMigrate, function(err) {
                    if(err) return callback(err);

                    var setNewSlotOwnerRecursive = function(nodes, slot, newNode, cb) {
                        if(nodes.length == 0) return cb();
                        var node = nodes.shift();
                        getClient(node).cluster("SETSLOT", slot, "NODE", newNode, function(err, resp) {
                            if (err) return cb(err);
                            setNewSlotOwnerRecursive(nodes, slot, newNode, cb)
                        })
                    }

                    var nodesToContact = Array.from(redisMasters);
                    nodesToContact.push(dest);

                    setNewSlotOwnerRecursive(nodesToContact, keySlot, destNodeID, function(err) {
                        if(err) callback("Failed to set node slot", err);
                        callback();
                    });
                });
            })
        })
    })
}
/**
 * Copyright 2016 DynomiteDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Sets
 *
 * Run
 * ---
 *
 * node set.js
 *
 * Expected output
 * ---------------
 *
 * --- BEGIN ---
 * sadd: employees Bob Sue Joe (3)
 * sadd: friends Bob Jane Joe (3)
 * scard: friends 3
 * sdiff: employees friends = Sue
 * sdiffstore: work_associates (1)
 * sinter: employees friends = Joe,Bob
 * sinterstore: friendly_coworkers (2)
 * sunion: employees friends = Jane,Joe,Bob,Sue
 * sunionstore: all_contacts (4)
 * smembers: friendly_coworkers Joe,Bob
 * sismember: friends Bob is friendly (1)
 * smove: Sue from work_associates to friendly_coworkers (1)
 * smembers: friendly_coworkers (Joe,Sue,Bob)
 * spop: friendly_coworkers (Joe)
 * smembers: friendly_coworkers (Sue,Bob)
 * srandmember: all_contacts Bob
 * smembers: all_contacts Bob,Sue,Joe,Jane
 * srem: all_contacts Jane (1)
 * sscan: all_contacts0,Joe,Sue
 * --- END ---
 *
 * Commands used in this file
 * --------------------------
 *
 * [x] SADD: Save one or more new members to a set
 * [x] SCARD: Get the number of members in a set
 * [x] SDIFF: Get the difference between a set and one or more other sets. The
 *     return value is an array of members that are in a set, but not the
 *     comparison sets.
 * [x] SDIFFSTORE: Save the difference between a set and one or more other sets
 *     in a new set
 * [x] SINTER: Get the intersection between two or more sets. The return value
 *     is an array with members that exist in all of the specified sets.
 * [x] SINTERSTORE: Save the intersection between two or more sets in a new set
 * [x] SISMEMBER: Check if a member exists in a set. Return 1 if member exists,
 *     else return 0.
 * [x] SMEMBERS: Get all members of a set
 * [x] SMOVE: Move a member from a source set to a destination set
 * [x] SPOP: Get one or more random members from a set. The member(s) returned
 *     are removed from the set.
 * [x] SRANDMEMBER: Get one or more random members from a set
 * [x] SREM: Remove one or more members from a set
 * [x] SSCAN: Return matching members by iterating over a set's members
 * [x] SUNION: Get the union of one or more sets. The return value is a
 *     deduplicated array of all members from all specified sets.
 * [x] SUNIONSTORE: Save the union of one or more sets in a new set
 */


var redis = require('redis');

// Connect to a DynomiteDB cluster
// var db = redis.createClient(8102, 'localhost');

// Connect to a local dynomitedb-redis instance
// var db = redis.createClient(22122, 'localhost');

// Connect to a single server Redis instance
var db = redis.createClient(6379, 'localhost');

db.on('error', function(err) {
    console.log(err);
    process.exit(1);
});

/*
 * Wait until we're connected to the Dynomite server before we issue commands
 */
db.on('connect', function() {
    // Let's add some keys
    console.log('--- BEGIN ---');
    createEmployeesSet('employees', 'Bob', 'Sue', 'Joe');
});

function createEmployeesSet(key, valA, valB, valC) {
    db.sadd(key, valA, valB, valC, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sadd: ' + key + ' ' + valA + ' ' + valB + ' ' + valC + ' (' + reply + ')');
            createFriendsSet('friends', 'Bob', 'Jane', 'Joe');
        } else {
            console.log('members already exist sadd: ' + key + ' (' + reply + ')');
            createFriendsSet('friends', 'Bob', 'Jane', 'Joe');
        }
    });
}

function createFriendsSet(key, valA, valB, valC) {
    db.sadd(key, valA, valB, valC, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sadd: ' + key + ' ' + valA + ' ' + valB + ' ' + valC + ' (' + reply + ')');
            countFriends('friends');
        } else {
            console.log('members already exist sadd: ' + key + ' (' + reply + ')');
            countFriends('friends');
        }
    });
}

function countFriends(key) {
    db.scard(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('scard: ' + key + ' ' + reply);
            workAssociatesOnly('employees', 'friends');
        } else {
            console.log('Unable to scard: ' + key);
        }
    });
}

function workAssociatesOnly(primarySet, secondarySet) {
    db.sdiff(primarySet, secondarySet, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sdiff: ' + primarySet + ' ' + secondarySet + ' = ' + reply);
            createWorkAssociatesSet('work_associates', 'employees', 'friends');
        } else {
            // Logic flow will come here as the key in lpushx does not exist
            console.log('Unable to sdiff:  (' + reply + ')');
            db.quit();
        }
    });
}

function createWorkAssociatesSet(newSet, primarySet, secondarySet) {
    db.sdiffstore(newSet, primarySet, secondarySet, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sdiffstore: ' + newSet + ' (' + reply + ')');
            friendlyCoworkers('employees', 'friends');
        } else {
            console.log('Unable to sdiffstore:  (' + reply + ')');
            db.quit();
        }
    });
}

function friendlyCoworkers(primarySet, secondarySet) {
    db.sinter(primarySet, secondarySet, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sinter: ' + primarySet + ' ' + secondarySet + ' = ' + reply);
            createFriendlyCoworkersSet('friendly_coworkers', 'employees', 'friends');
        } else {
            console.log('Unable to sinter:  (' + reply + ')');
            db.quit();
        }
    });
}

function createFriendlyCoworkersSet(newSet, primarySet, secondarySet) {
    db.sinterstore(newSet, primarySet, secondarySet, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sinterstore: ' + newSet + ' (' + reply + ')');
            allContacts('employees', 'friends');
        } else {
            console.log('Unable to sinterstore:  (' + reply + ')');
            db.quit();
        }
    });
}

function allContacts(primarySet, secondarySet) {
    db.sunion(primarySet, secondarySet, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sunion: ' + primarySet + ' ' + secondarySet + ' = ' + reply);
            createAllContactsSet('all_contacts', 'employees', 'friends');
        } else {
            console.log('Unable to sunion:  (' + reply + ')');
            db.quit();
        }
    });
}

function createAllContactsSet(newSet, primarySet, secondarySet) {
    db.sunionstore(newSet, primarySet, secondarySet, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sunionstore: ' + newSet + ' (' + reply + ')');
            viewFriendlyCoworkers('friendly_coworkers');
        } else {
            console.log('Unable to sunionstore:  (' + reply + ')');
            db.quit();
        }
    });
}

function viewFriendlyCoworkers(key) {
    db.smembers(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('smembers: ' + key + ' ' + reply);
            isBobFriendly('friends', 'Bob');
        } else {
            console.log('Unable to smembers: ' + key);
            db.quit();
        }
    });
}

function isBobFriendly(key, value) {
    db.sismember(key, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sismember: ' + key + ' ' + value + ' is friendly (' + reply + ')');
            befriendSue('work_associates', 'friendly_coworkers', 'Sue');
        } else {
            console.log('sismember: ' + key + ' ' + value + ' is not friendly');
            db.quit();
        }
    });
}

function befriendSue(source, destination, member) {
    db.smove(source, destination, member, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('smove: ' + member + ' from ' + source + ' to ' + destination + ' (' + reply + ')');
            viewFriendlyCoworkersAfterMove('friendly_coworkers');
        } else {
            console.log('Unable to smove: ' + member + ' from ' + source + ' to ' + destination);
            db.quit();
        }
    });
}

function viewFriendlyCoworkersAfterMove(key) {
    db.smembers(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('smembers: ' + key + ' (' + reply + ')');
            getAndRemoveOneFriend('friendly_coworkers');
        } else {
            console.log('Unable to smembers: ' + key);
            db.quit();
        }
    });
}

function getAndRemoveOneFriend(key) {
    db.spop(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('spop: ' + key + ' (' + reply + ')');
            viewFriendlyCoworkersAfterRemove('friendly_coworkers');
        } else {
            console.log('Unable to spop: ' + key);
            db.quit();
        }
    });
}

function viewFriendlyCoworkersAfterRemove(key) {
    db.smembers(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('smembers: ' + key + ' (' + reply + ')');
            getRandomPerson('all_contacts', 1);
        } else {
            console.log('Unable to smembers: ' + key);
            db.quit();
        }
    });
}

function getRandomPerson(key, count) {
    db.srandmember(key, count, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('srandmember: ' + key + ' ' + reply);
            viewAllContactsAfterGetRand('all_contacts');
        } else {
            console.log('Unable to srandmember: ' + key);
            db.quit();
        }
    });
}

function viewAllContactsAfterGetRand(key) {
    db.smembers(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('smembers: ' + key + ' ' + reply);
            removeJane('all_contacts', 'Jane');
        } else {
            console.log('Unable to smembers: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function removeJane(key, member) {
    db.srem(key, member, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('srem: ' + key + ' ' + member + ' (' + reply + ')');
            getAllContentsEndWithe('all_contacts', 'e');
        } else {
            console.log('Unable to srem: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function getAllContentsEndWithe(key, value) {
    db.sscan(key, 0, 'MATCH', '*' + value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('sscan: ' + key + reply);
            console.log('--- END ---');
            db.quit();
        } else {
            console.log('Unable to sscan: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

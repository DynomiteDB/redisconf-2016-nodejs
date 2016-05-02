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
 * Lists
 *
 * Run
 * ---
 *
 * node list.js
 *
 * Expected output
 * ---------------
 *
 * --- BEGIN ---
 * lpush: blog:recent_posts 100 7 500
 * lrange: blog:recent_posts 500,7
 * lpushx: blog:recent_posts 800
 * Unable to lpushx: list_does_not_exist does not exist (0)
 * lpop: blog:recent_posts 800
 * rpop: blog:recent_posts 100
 * lset: blog:recent_posts[0] 1001 (OK)
 * lrange: blog:recent_posts 1001,7
 * rpush: blog:recent_posts (4)
 * lrange: blog:recent_posts 7,20000,30000
 * Unable to rpushx: fake_list does not exist (0)
 * llen: blog:recent_posts 4
 * linsert: blog:recent_posts (5)
 * lindex: blog:recent_posts[1] 1002
 * lrange: blog:recent_posts 1001,1002,7,20000,30000
 * lrem: blog:recent_posts (1)
 * ltrim: blog:recent_posts (OK)
 * lrange: blog:recent_posts 1002,7,20000
 * lpush: blog:archive 1 (1)
 * rpoplpush: blog:recent_posts to blog:archive (20000)
 * lrange: blog:recent_posts 1002,7
 * lrange: blog:archive 20000,1
 * --- END ---
 *
 * Commands used in this file
 * --------------------------
 *
 * [x] LINDEX: Get an item from the list located at the specified index
 * [x] LINSERT: Save a new element before or after the specified item
 * [x] LLEN: Get the number of items in the list
 * [x] LPOP: Get the first item from the list and remove it from the list
 * [x] LPUSH: Save a new item as the 1st item in a list (i.e. the left side)
 * [x] LPUSHX: Save a new item as the 1st item in a list only if the list already exists
 * [x] LRANGE: Get one or more item in the list within the specified index range
 * [x] LREM: Remove one or more occurrences of an item from a list
 * [x] LSET: Update the value of an item at a specified index
 * [x] LTRIM: Trim a list by deleting items within the specified range
 * [x] RPOP: Remove the last item in a list (i.e. from the right side)
 * [x] RPOPLPUSH: Remove the last item from one list and save it as the 1st
 *     item in another list (Requires both keys to be on the same server)
 * [x] RPUSH: Save one or more items at the end of a list (i.e. the right side)
 * [x] RPUSHX: Save one or more items at the end of a list (i.e. the right
 *     side), only if the key already exists and holds a list
 *
 * The following list commands are not supported by DynomiteDB as they do not
 * work well in a clustered environment:
 * [ ] BLPOP: Not supported by DynomiteDB
 * [ ] BRPOP: Not supported by DynomiteDB
 * [ ] BRPOPLPUSH: Not supported by DynomiteDB
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
    pushInitialRecentPosts('blog:recent_posts', 100, 7, 500);
});

function pushInitialRecentPosts(key, valA, valB, valC) {
    db.lpush(key, valA, valB, valC, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lpush: ' + key + ' ' + valA + ' ' + valB + ' ' + valC);
            getTwoRecentPosts('blog:recent_posts');
        } else {
            console.log('Unable to lpush: ' + key);
            db.quit();
        }
    });
}

function getTwoRecentPosts(key) {
    db.lrange(key, 0, 1, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('lrange: ' + key + ' ' + reply);
            pushPostsToExistingList('blog:recent_posts', 800);
        } else {
            console.log('Unable to lrange: ' + key);
            db.quit();
        }
    });
}

function pushPostsToExistingList(key, value) {
    db.lpushx(key, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lpushx: ' + key + ' ' + value);
            preventPushToNonList('list_does_not_exist', 88);
        } else {
            console.log('Unable to lpushx: ' + key);
        }
    });
}

function preventPushToNonList(key, valA) {
    db.lpushx(key, valA, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lpushx: ' + key + ' ' + valA);
            db.quit();
        } else {
            // Logic flow will come here as the key in lpushx does not exist
            console.log('Unable to lpushx: ' + key + ' does not exist (' + reply + ')');
            getMostRecentPost('blog:recent_posts');
        }
    });
}

function getMostRecentPost(key) {
    db.lpop(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lpop: ' + key + ' ' + reply);
            getOldestPost('blog:recent_posts')
        } else {
            console.log('Unable to lpop: ' + key);
            db.quit();
        }
    });
}

function getOldestPost(key) {
    db.rpop(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('rpop: ' + key + ' ' + reply);
            changeRecentPost('blog:recent_posts', 0, 1001);
        } else {
            console.log('Unable to rpop: ' + key);
            db.quit();
        }
    });
}

function changeRecentPost(key, index, value) {
    db.lset(key, index, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lset: ' + key + '[' + index + '] ' + value + ' (' + reply + ')');
            viewListAfterLset('blog:recent_posts');
        } else {
            console.log('Unable to lset: ' + key);
            db.quit();
        }
    });
}

function viewListAfterLset(key) {
    db.lrange(key, 0, -1, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lrange: ' + key + ' ' + reply);
            pushToEndOfList('blog:recent_posts', 20000, 30000);
        } else {
            console.log('Unable to lrange: ' + key);
            db.quit();
        }
    });
}

function pushToEndOfList(key, valA, valB) {
    db.rpush(key, valA, valB, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('rpush: ' + key + ' (' + reply + ')');
            viewThreeOldestItems('blog:recent_posts');
        } else {
            console.log('Unable to rpush: ' + key);
            db.quit();
        }
    });
}

function viewThreeOldestItems(key) {
    db.lrange(key, -3, -1, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lrange: ' + key + ' ' + reply);
            preventRightPushToNonList('fake_list', 87);
        } else {
            console.log('Unable to get: ' + key);
            db.quit();
        }
    });
}

function preventRightPushToNonList(key, value) {
    db.rpushx(key, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('rpushx: ' + key + ' (' + reply + ')');
            db.quit();
        } else {
            console.log('Unable to rpushx: ' + key + ' does not exist (' + reply + ')');
            getListLength('blog:recent_posts');
        }
    });
}

function getListLength(key) {
    db.llen(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('llen: ' + key + ' ' + reply);
            insertSecondMostRecent('blog:recent_posts', 'AFTER', 1001, 1002);
        } else {
            console.log('Unable to llen: ' + key);
            db.quit();
        }
    });
}

function insertSecondMostRecent(key, insert, pivot, value) {
    db.linsert(key, insert, pivot, value, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('linsert: ' + key + ' (' + reply + ')');
            getSecondItem('blog:recent_posts', 1);
        } else {
            console.log('Unable to linsert: ' + key);
            db.quit();
        }
    });
}

function getSecondItem(key, index) {
    db.lindex(key, index, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('lindex: ' + key + '[' + index + '] ' + reply);
            viewListAfterLindex('blog:recent_posts');
        } else {
            console.log('Unable to lindex: ' + key);
            db.quit();
        }
    });
}

function viewListAfterLindex(key) {
    db.lrange(key, 0, -1, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lrange: ' + key + ' ' + reply);
            removeItem('blog:recent_posts', 1, 1001);
        } else {
            console.log('Unable to lrange: ' + key);
            db.quit();
        }
    });
}

function removeItem(key, count, value) {
    db.lrem(key, count, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lrem: ' + key + ' (' + reply + ')');
            limitToTopThree('blog:recent_posts', 0, 2);
        } else {
            console.log('Unable to lrem: ' + key);
            db.quit();
        }
    });
}

function limitToTopThree(key, start, stop) {
    db.ltrim(key, start, stop, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('ltrim: ' + key + ' (' + reply + ')');
            viewListAfterTrim('blog:recent_posts');
        } else {
            console.log('Unable to ltrim: ' + key);
            db.quit();
        }
    });
}

function viewListAfterTrim(key) {
    db.lrange(key, 0, -1, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lrange: ' + key + ' ' + reply);
            createPostArchive('blog:archive', 1);
        } else {
            console.log('Unable to lrange: ' + key);
            db.quit();
        }
    });
}

function createPostArchive(key, value) {
    db.lpush(key, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lpush: ' + key + ' ' + value + ' (' + reply + ')');
            archiveOldestPost('blog:recent_posts', 'blog:archive');
        } else {
            console.log('Unable to lpush: ' + key);
            db.quit();
        }
    });
}

function archiveOldestPost(source, destination) {
    db.rpoplpush(source, destination, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('rpoplpush: ' + source + ' to ' + destination + ' (' + reply + ')');
            viewRecentPosts('blog:recent_posts');
        } else {
            console.log('Unable to lpush: ' + source);
            db.quit();
        }
    });
}

function viewRecentPosts(key) {
    db.lrange(key, 0, -1, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lrange: ' + key + ' ' + reply);
            viewArchivePosts('blog:archive');
        } else {
            console.log('Unable to lrange: ' + key);
            db.quit();
        }
    });
}

function viewArchivePosts(key) {
    db.lrange(key, 0, -1, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('lrange: ' + key + ' ' + reply);
            console.log('--- END ---');
            db.quit();
        } else {
            console.log('Unable to lrange: ' + key);
            db.quit();
        }
    });
}
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
 * Sorted sets
 *
 * Run
 * ---
 *
 * node sorted_set.js
 *
 * Expected output
 * ---------------
 *
 * --- BEGIN ---
 * zadd: tags:category=startups (3)
 * zadd: tags:category=startups (3)
 * zadd: tags:category=marketing (3)
 * zcard: tags:category=startups 6
 * zcount: tags:category=startups 5
 * zincrby: tags:category=marketing Growth (414)
 * zinterstore: tags:category=startups tags:category=marketing = 2
 * zrange: tags:category=common Growth,Customers
 * zrevrange: tags:category=common Customers,Growth
 * zadd: tags 3
 * zadd: tags 3
 * zadd: tags 3
 * zadd: tags 3
 * zrange: tags (Awareness,Customer Knowledge,Customers,Founders,Funding,Go To Market,Growth,Hiring,Market Knowledge,Product/Market Fix,Strategy,Traction)
 * zlexcount: tags [A to [I 8
 * zrangebylex: tags from [A to [I Awareness,Customer Knowledge,Customers,Founders,Funding,Go To Market,Growth,Hiring
 * zrevrangebylex: tags from [I to [A Hiring,Growth,Go To Market,Funding,Founders,Customers,Customer Knowledge,Awareness
 * zrangebyscore: tags:category=startups from 1 to 50 Traction,1,Growth,25,Funding,31
 * zrevrangebyscore: tags:category=startups from 500 to 50 Product/Market Fit,321,Customers,67,Market Knowledge,56
 * zrank: tags:category=startups Customers 4
 * zrevrank: tags:category=startups Customers 1
 * zscore: tags:category=startups Customers 67
 * zunionstore: tags:category=union
 * zrange: tags:category=union Traction,Funding,Market Knowledge,Awareness,Product/Market Fit,Growth,Customers
 * zscan: tags:category=union *o* 0,Traction,1,Market Knowledge,56,Product/Market Fit,321,Growth,439,Customers,742
 * zrem: tags:category=union Product/Market Fit (1)
 * zrange: tags:category=union Traction,1,Funding,31,Market Knowledge,56,Awareness,233,Growth,439,Customers,742
 * zremrangebylex: tags [A to [H (7)
 * zrange: tags Hiring,1,Market Knowledge,1,Product/Market Fix,1,Strategy,1,Traction,1
 * zremrangebyrank: tags:category=union 0 to 0 (1)
 * zrange: tags:category=union Funding,31,Market Knowledge,56,Awareness,233,Growth,439,Customers,742
 * zremrangebyscore: tags:category=union 400 to 900 (2)
 * zrange: tags:category=union Funding,31,Market Knowledge,56,Awareness,233
 * --- END ---
 *
 * Commands used in this file
 * --------------------------
 *
 * [x] ZADD: Save one or more new members to a sorted set, or update a member's
 *     score if it already exists in the sorted set
 * [x] ZCARD: Get the number of members in a sorted set
 * [x] ZCOUNT: Get the number of members with a score between the min and max
 *     value specified
 * [x] ZINCRBY: Increment the score of a member
 * [x] ZINTERSTORE: Save the intersection between two or more sets in a new set
 * [x] ZLEXCOUNT: Get the number of members between a lexicographical range
 * [x] ZRANGE: Get members within an indexed range sorted low to high
 * [x] ZREVRANGE: Get members within an indexed range sorted high to low
 * [x] ZRANGEBYLEX: Get members within a lexicographical range sorted low to high
 * [x] ZREVRANGEBYLEX: Get members within a lexicographical range sorted high to low
 * [x] ZRANGEBYSCORE: Get members who's scores are within the specified ranged
 *     sorted low to high
 * [x] ZREVRANGEBYSCORE: Get members who's scores are within the specified ranged
 *     sorted high to low
 * [x] ZRANK: Get the index of a member
 * [x] ZREVRANK: Get the index of a member when the sorted set is sorted high to low
 * [x] ZREM: Remove one or more members from a sorted set
 * [x] ZREMRANGEBYLEX: Delete members within a lexicographical range
 * [x] ZREMRANGEBYRANK: Delete members within the specified indexes
 * [x] ZREMRANGEBYSCORE: Delete members who's scores are within the specified
 *     range
 * [x] ZSCORE: Get the score of a member
 * [x] ZUNIONSTORE: Save the union of one or more sorted sets in a new
 *     sorted set
 * [x] ZSCAN: Iterate over the members of the sorted set
 */


var redis = require('redis');

// Connect to a DynomiteDB cluster
// var db = redis.createClient(8102, 'localhost');

// Connect to a local dynomitedb-redis instance
// var db = redis.createClient(22122, 'localhost');

// Connect to a single server Redis instance
var db = redis.createClient(6379, 'localhost');
var startupTagsKey = 'tags:category=startups';
var marketingTagsKey = 'tags:category=marketing';
var commonTagsKeys = 'tags:category=common';
var unionTagsKey = 'tags:category=union';
var allTags = 'tags';

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
    createStartupTags(startupTagsKey,
        31, 'Funding',
        25, 'Growth',
        67, 'Customers',
        true);
});

function createStartupTags(key, scoreA, memA, scoreB, memB, scoreC, memC, re) {
    db.zadd(key, scoreA, memA, scoreB, memB, scoreC, memC, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zadd: ' + key + ' (' + reply + ')');
            if (re) {
                addMoreStartTags();
            } else {
                callNext();
            }
        } else {
            console.log('members already exist zadd: ' + key + ' (' + reply + ')');
            if (re) {
                addMoreStartTags();
            } else {
                callNext();
            }
        }

        function addMoreStartTags() {
            createStartupTags(startupTagsKey,
                321, 'Product/Market Fit',
                1, 'Traction',
                56, 'Market Knowledge',
                false);
        }

        function callNext() {
            createMarketingTags(marketingTagsKey,
                409, 'Growth',
                675, 'Customers',
                233, 'Awareness');
        }
    });
}

function createMarketingTags(key, scoreA, memA, scoreB, memB, scoreC, memC) {
    db.zadd(key, scoreA, memA, scoreB, memB, scoreC, memC, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zadd: ' + key + ' (' + reply + ')');
            callNext();
        } else {
            console.log('members already exist zadd: ' + key + ' (' + reply + ')');
            callNext();
        }

        function callNext() {
            countStartupTags(startupTagsKey);
        }
    });
}

function countStartupTags(key) {
    db.zcard(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zcard: ' + key + ' ' + reply);
            countStartupTagsWithinScoreRange(startupTagsKey, 1, 100);
        } else {
            console.log('Unable to zcard: ' + key);
        }
    });
}

function countStartupTagsWithinScoreRange(key, min, max) {
    db.zcount(key, min, max, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zcount: ' + key + ' ' + reply);
            incrMarketingGrowth(marketingTagsKey, 5, 'Growth');
        } else {
            console.log('Unable to zcount: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function incrMarketingGrowth(key, increment, member) {
    db.zincrby(key, increment, member, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zincrby: ' + key + ' ' + member + ' (' + reply + ')');
            createCommonTagsZSet(commonTagsKeys, 2, startupTagsKey, marketingTagsKey);
        } else {
            console.log('Unable to zincrby:  (' + reply + ')');
            db.quit();
        }
    });
}

function createCommonTagsZSet(destination, numKeys, zsetA, zsetB) {
    db.zinterstore(destination, numKeys, zsetA, zsetB, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zinterstore: ' + zsetA + ' ' + zsetB + ' = ' + reply);
            viewAllCommonTags(commonTagsKeys);
        } else {
            console.log('Unable to zinterstore:  (' + reply + ')');
            db.quit();
        }
    });
}

function viewAllCommonTags(key) {
    db.zrange(key, 0, -1, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrange: ' + key + ' ' + reply);
            viewAllCommonTagsRev(commonTagsKeys);
        } else {
            console.log('Unable to zrange:  (' + reply + ')');
            db.quit();
        }
    });
}

function viewAllCommonTagsRev(key) {
    db.zrevrange(key, 0, -1, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrevrange: ' + key + ' ' + reply);
            createTagsZSet(allTags, 1, 'Funding', 1, 'Growth', 1, 'Customers', 1);
        } else {
            console.log('Unable to zrevrange:  (' + reply + ')');
            db.quit();
        }
    });
}

function createTagsZSet(key, scoreA, memA, scoreB, memB, scoreC, memC, iteration) {
    db.zadd(key, scoreA, memA, scoreB, memB, scoreC, memC, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zadd: ' + key + ' ' + reply);

            if (iteration >= 1 && iteration <= 3) {
                addMoreTags(iteration);
            } else {
                viewAllTagsZSet(allTags);
            }
        } else {
            console.log('Unable to zadd: ' + key);
            if (iteration >= 1 && iteration <= 3) {
                addMoreTags(iteration);
            } else {
                db.quit();
            }
        }

        function addMoreTags(iteration) {
            if (iteration === 1) {
                createTagsZSet(allTags,
                    1, 'Go To Market',
                    1, 'Strategy',
                    1, 'Customer Knowledge',
                    2);
            } else if (iteration === 2) {
                createTagsZSet(allTags,
                    1, 'Product/Market Fix',
                    1, 'Traction',
                    1, 'Market Knowledge',
                    3);
            } else if (iteration === 3) {
                createTagsZSet(allTags,
                    1, 'Awareness',
                    1, 'Founders',
                    1, 'Hiring',
                    4);
            }
        }
    });
}

function viewAllTagsZSet(key) {
    db.zrange(key, 0, -1, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrange: ' + key + ' (' + reply + ')');
            countAllTagsAtoI(allTags, '[A', '[I');
        } else {
            console.log('Unable to zrange:  (' + reply + ')');
            db.quit();
        }
    });
}

function countAllTagsAtoI(key, min, max) {
    db.zlexcount(key, min, max, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zlexcount: ' + key + ' ' + min + ' to ' + max + ' ' + reply);
            getAllTagsAtoI(allTags, '[A', '[I');
        } else {
            console.log('Unable to zlexcount: ' + key);
            db.quit();
        }
    });
}

function getAllTagsAtoI(key, min, max) {
    db.zrangebylex(key, min, max, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrangebylex: ' + key + ' from ' + min + ' to ' + max + ' ' + reply);
            getAllTagsItoA(allTags, '[I', '[A');
        } else {
            console.log('Unable to zrangebylex: ' + key);
            db.quit();
        }
    });
}

function getAllTagsItoA(key, max, min) {
    db.zrevrangebylex(key, max, min, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrevrangebylex: ' + key + ' from ' + max + ' to ' + min + ' ' + reply);
            getStartupTags1to50(startupTagsKey, 1, 50, 'WITHSCORES');
        } else {
            console.log('Unable to zrevrangebylex: ' + key);
            db.quit();
        }
    });
}

function getStartupTags1to50(key, min, max, withscores) {
    db.zrangebyscore(key, min, max, withscores, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrangebyscore: ' + key + ' from ' + min + ' to ' + max + ' ' + reply);
            getStartupTags500to50(startupTagsKey, 500, 50, 'WITHSCORES');
        } else {
            console.log('Unable to zrangebyscore: ' + key);
            db.quit();
        }
    });
}

function getStartupTags500to50(key, max, min, withscores) {
    db.zrevrangebyscore(key, max, min, withscores, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrevrangebyscore: ' + key + ' from ' + max + ' to ' + min + ' ' + reply);
            getMemberRank(startupTagsKey, 'Customers');
        } else {
            console.log('Unable to zrevrangebyscore: ' + key);
            db.quit();
        }
    });
}

function getMemberRank(key, member) {
    db.zrank(key, member, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrank: ' + key + ' ' + member + ' ' + reply);
            getMemberRevRank(startupTagsKey, 'Customers');
        } else {
            console.log('Unable to zrank: ' + key);
            db.quit();
        }
    });
}

function getMemberRevRank(key, member) {
    db.zrevrank(key, member, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrevrank: ' + key + ' ' + member + ' ' + reply);
            getCustomersScore(startupTagsKey, 'Customers');
        } else {
            console.log('Unable to zrevrank: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function getCustomersScore(key, member) {
    db.zscore(key, member, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zscore: ' + key + ' ' + member + ' ' + reply);
            combineStartupsMarketing(unionTagsKey, 2, startupTagsKey, marketingTagsKey);
        } else {
            console.log('Unable to zscore: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function combineStartupsMarketing(key, numKeys, keyA, keyB) {
    db.zunionstore(key, numKeys, keyA, keyB, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zunionstore: ' + key);
            viewUnionTags(unionTagsKey);
        } else {
            console.log('Unable to zunionstore: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function viewUnionTags(key) {
    db.zrange(key, 0, -1, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrange: ' + key + ' ' + reply);
            scanUnionTags(unionTagsKey, 0, '*o*');
        } else {
            console.log('Unable to zrange: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function scanUnionTags(key, cursor, pattern) {
    db.zscan(key, cursor, 'MATCH', pattern, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zscan: ' + key + ' ' + pattern + ' ' + reply);
            removePMFit(unionTagsKey, 'Product/Market Fit');
        } else {
            console.log('Unable to zscan: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function removePMFit(key, member) {
    db.zrem(key, member, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrem: ' + key + ' ' + member + ' (' + reply + ')');
            viewUnionTagsAfterRemove(unionTagsKey);
        } else {
            console.log('Unable to zrem: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function viewUnionTagsAfterRemove(key) {
    db.zrange(key, 0, -1, 'WITHSCORES', function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrange: ' + key + ' ' + reply);
            removeAtoHUnionTags(allTags, '[A', '[H');
        } else {
            console.log('Unable to zrange: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function removeAtoHUnionTags(key, min, max) {
        db.zremrangebylex(key, min, max, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zremrangebylex: ' + key + ' ' + min + ' to ' + max + ' (' + reply + ')');
            viewAllTagsAfterRemoveAtoH(allTags);
        } else {
            console.log('Unable to zremrangebylex: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function viewAllTagsAfterRemoveAtoH(key) {
    db.zrange(key, 0, -1, 'WITHSCORES', function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrange: ' + key + ' ' + reply);
            removeFirstMemberUnionTags(unionTagsKey, 0, 0);
        } else {
            console.log('Unable to zrange: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function removeFirstMemberUnionTags(key, start, stop) {
    db.zremrangebyrank(key, start, stop, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zremrangebyrank: ' + key + ' ' + start + ' to ' + stop + ' (' + reply + ')');
            viewUnionTagsAfterRemoveFirst(unionTagsKey);
        } else {
            console.log('Unable to zremrangebyrank: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function viewUnionTagsAfterRemoveFirst(key) {
    db.zrange(key, 0, -1, 'WITHSCORES', function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrange: ' + key + ' ' + reply);
            removeScore400to900MemberUnionTags(unionTagsKey, 400, 900);
        } else {
            console.log('Unable to zrange: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function removeScore400to900MemberUnionTags(key, min, max) {
    db.zremrangebyscore(key, min, max, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zremrangebyscore: ' + key + ' ' + min + ' to ' + max + ' (' + reply + ')');
            viewUnionTagsAfterRemove400to900(unionTagsKey);
        } else {
            console.log('Unable to zremrangebyscore: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function viewUnionTagsAfterRemove400to900(key) {
    db.zrange(key, 0, -1, 'WITHSCORES', function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('zrange: ' + key + ' ' + reply);
            console.log('--- END ---');
            db.quit();
        } else {
            console.log('Unable to zrange: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

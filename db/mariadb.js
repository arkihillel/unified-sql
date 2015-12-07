var Client = require('mariasql');
var async = require('async');

var executeQueries = function(host, port, user, password, db, queries, options, cb) {
	var c = new Client();

	c.connect({
		host: host,
		user: user,
		password: password,
		db: db,
		port: port
	});

	c.on('error', function(err){
		return cb(err);
	});

	c.on('ready', function(){
		var result = [];

		async.eachSeries(queries, function(query, callback) {

			c.query(query, function(err, rows) {
				if (err) 
					result.push(new Error(err));
				else 
					result.push(formatOutput(rows));

				return callback();
			});
		}, function() {
			c.end();
			return cb(null, result);
		});
	});
};

var testConnection = function(host, port, user, password, db, options, cb) {
	var c = new Client();

	c.connect({
		host: host,
		user: user,
		password: password,
		db: db,
		port: port
	});

	c.on('ready', function(){
		c.end();
		return cb();
	});

	c.on('error', function(err){
		return cb(err);
	});

};

var formatOutput = function(output) {
	var columns = Object.keys(output[0]);
	var rows = [];
	for (var i = 0; i < output.length; i++) {
		var row = [];
		for (var value in output[i]) {
			row.push(output[i][value]);
		}
		rows.push(row);
	}

	return {
		columns: columns,
		rows: rows
	};
};

module.exports = {
	executeQueries: executeQueries,
	testConnection: testConnection
};
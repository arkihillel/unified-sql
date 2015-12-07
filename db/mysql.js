var mysql = require('mysql');
var async = require('async');

var executeQueries = function(host, port, user, password, db, queries, options, cb) {
	var connection = mysql.createConnection({
		host: host,
		user: user,
		password: password,
		database: db,
		port: port || 3306
	});
	connection.connect(function(err){
		if(err) return cb(err);

		var result = [];

		async.eachSeries(queries, function(query, callback) {
			connection.query(query, function(err, rows, fields) {
				if (err) 
					result.push(new Error(err));
				else 
					result.push(formatOutput(rows));

				return callback();
			});
		}, function() {
			connection.end();
			return cb(null, result);
		});

	});
};

var testConnection = function(host, port, user, password, db, options, cb) {
	var connection = mysql.createConnection({
		host: host,
		user: user,
		password: password,
		database: db,
		port: port || 3306
	});

	connection.connect(function(err){
		if(err) return cb(err);
		connection.end();
		return cb();
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
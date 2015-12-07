var pg = require('pg');
var async = require('async');

var executeQueries = function(host, port, user, password, db, queries, options, cb) {
	var conString = 'postgres://' + user + ':' + password + '@' + host + ':' + (port || 5432) + '/' + db;

	var client = new pg.Client(conString);
	client.connect(function(err) {
		if (err) return cb(err);

		var result = [];

		async.eachSeries(queries, function(query, callback) {
			client.query(query, function(err, queryResult) {
				if (err) 
					result.push(new Error(err));
				else 
					result.push(formatOutput(queryResult));

				return callback();
			});
		}, function() {
			client.end();
			return cb(null, result);
		});
	});
};

var testConnection = function(host, port, user, password, db, options, cb) {
	var conString = 'postgres://' + user + ':' + password + '@' + host + ':' + (port || 5432) + '/' + db;

	var client = new pg.Client(conString);
	client.connect(function(err) {
		if (err) return cb(err);
		client.end();
		return cb();
	});
};

var formatOutput = function(output) {
	var columns = [];
	for (var i = 0; i < output.fields.length; i++) {
		columns.push(output.fields[i].name);
	}


	var rows = [];
	for (var i = 0; i < output.rows.length; i++) {
		var row = [];
		for (var value in output.rows[i]) {
			row.push(output.rows[i][value]);
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


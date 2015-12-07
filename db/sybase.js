var Sybase = require('sybase');
var async = require('async');

var executeQueries = function(host, port, user, password, db, queries, options, cb) {
	var db = new Sybase(host, port, db, user, password);
 
	db.connect(function (err) {
		if (err) return cb(err);

		var result = [];

		async.eachSeries(queries, function(query, callback) {
			db.query(query, function (err, data) {

				if (err) 
					result.push(new Error(err));
				else 
					result.push(formatOutput(data));

				return callback();
			});
			
		}, function() {
			db.disconnect();
			return cb(null, result);
		});
	});
};

var testConnection = function(host, port, user, password, db, options, cb) {
	var db = new Sybase(host, port, db, user, password);

	db.connect(function (err) {
		if (err) return cb(err);
		db.disconnect();
		return cb();
	});
};

var formatOutput = function(output) {
	var columns = [];
	var rows = [];

	for (var i = 0; i < output.length; i++) {
		rows[i] = [];
		for (var j = 0; j < Object.keys(output[i]).length; j++) {
			if(columns.indexOf(Object.keys(output[i])[j]) ==-1) columns.push(Object.keys(output[i])[j]);
			rows[i][columns.indexOf(Object.keys(output[i])[j])] = output[i][Object.keys(output[i])[j]]
		}
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
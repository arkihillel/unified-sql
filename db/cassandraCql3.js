var cassandra = require('cassandra-driver');
var async = require('async');

var executeQueries = function(host, port, user, password, db, queries, options, cb) {
	
	var client = new cassandra.Client({
		contactPoints: [host], 
		authProvider: new cassandra.auth.PlainTextAuthProvider(user, password),
		keyspace: db,
		protocolOptions: {
			port: port
		}
	});

	client.connect(function(err){
		if(err) return cb(err);
		
		var result = [];

		async.eachSeries(queries, function(query, callback) {
			client.execute(query, function(err, rows) {

				if (err) 
					result.push(new Error(err));
				else 
					result.push(formatOutput(rows.rows));

				return callback();
			});
		}, function() {
			client.shutdown(function () {
				return cb(null, result);
			});
		});
	});
};


var testConnection = function(host, port, user, password, db, options, cb) {
	// var contactPoints = host.split(',');

	var client = new cassandra.Client({
		contactPoints: [host], 
		authProvider: new cassandra.auth.PlainTextAuthProvider(user, password),
		keyspace: db,
		protocolOptions: {
			port: port
		}
	});

	client.connect(function(err){
		if(err) return cb(err);
		client.shutdown(function(){
			return cb();
		})
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

	for (var i = 0; i < rows.length; i++) {
		if(rows[i].length !== columns.length) rows[i][columns.length - 1] = null;
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



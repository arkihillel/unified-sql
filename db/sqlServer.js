var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var async = require('async');

var executeQueries = function(host, port, user, password, db, queries, options, cb) {
	var isAzure = (options && options.isAzure) ? true : false;
	var config = {
		server: host,
		userName: user,
		password: password,
		options: {
			port: port || 1433,
			database: db,
			readOnlyIntent: true,
			useColumnNames: true,
			encrypt: (options && options.isAzure) ? true : false,
			requestTimeout: 0
		}
	};
	var connection = new Connection(config);
	connection.on('connect', function(err) {
		if(err) return cb(err);

		var result = [];

		async.eachSeries(queries, function(query, callback) {
			var queryResult = {
				columns: [],
				rows: []
			};

			request = new Request(query, function(err){
				if(err) 
					result.push(new Error(err));
				else
					result.push(queryResult);
				
				return callback();
			});

			request.on('columnMetadata', function (columns) {
				queryResult.columns = Object.keys(columns);
			});

			request.on('row', function (columns) {
				var row = [];
				for(var column in columns) row.push(columns[column].value);
				queryResult.rows.push(row);
			});

			connection.execSql(request);

		}, function() {
			connection.close();
			return cb(null, result);
		});
		
	});
};

var testConnection = function(host, port, user, password, db, options, cb) {
	var config = {
		server: host,
		userName: user,
		password: password,
		options: {
			port: port || 1433,
			database: db,
			rowCollectionOnDone: true,
			rowCollectionOnRequestCompletion: true,
			encrypt: (options && options.isAzure) ? true : false
		}
	};
	var connection = new Connection(config);
	connection.on('connect', function(err) {
		if(err) return cb(err);
		connection.close();
		return cb();
	});
};

module.exports = {
	executeQueries: executeQueries,
	testConnection: testConnection
};
var oracledb = require('oracledb');
var async = require('async');

var executeQueries = function(host, port, user, password, db, queries, options, cb) {

	oracledb.getConnection(
	  {
		user          : user,
		password      : password,
		connectString : host + '/' + db + ':' + port || 1521
	  },
	  function(err, connection)
	  {
		if (err) return cb(err);

		var result = [];

		async.eachSeries(queries, function(query, callback) {
			connection.execute(query, [], function(err, queryResult) {
				if (err) 
					result.push(new Error(err));
				else 
					result.push(formatOutput(queryResult));

				return callback();
			});
		}, function() {
			connection.release(function(err){
				if (err) return cb(err);
				return cb(null, result);
			});
		});
	});
}

var testConnection = function(host, port, user, password, db, options, cb) {

	oracledb.getConnection(
	  {
		user          : user,
		password      : password,
		connectString : host + '/' + db + ':' + port || 1521
	  },
	  function(err, connection)
	  {
		if (err) return cb(err);

		connection.release(function(err){
			if (err) return cb(err);
			return cb();
		});
	});
}

var formatOutput = function(output){
	var columns = [];
	for(var column in output.metaData) columns.push(output.metaData[column].name);

	return {rows: output.rows, columns: columns};
}

module.exports = {
	executeQueries: executeQueries,
	testConnection: testConnection
}
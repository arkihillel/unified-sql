var ibmdb = require('ibm_db');
var async = require('async');

var executeQueries = function(host, port, user, password, db, queries, options, cb) {
	ibmdb.open('DRIVER={DB2};DATABASE=' + db + ';HOSTNAME=' + host + ';UID=' + user + ';PWD=' + password + ';PORT=' + (port || 50001) + ';PROTOCOL=TCPIP', function (err,conn) {
		if (err) return cb(err);


		var result = [];

		async.eachSeries(queries, function(query, callback) {
			conn.query(query, function(err, rows) {

				if (err) 
					result.push(new Error(err));
				else 
					result.push(formatOutput(rows));

				return callback();
			});
		}, function() {
			conn.close(function () {
				console.log('done');
				return cb(null, result);
			});
		});
	});
};

var testConnection = function(host, port, user, password, db, options, cb) {
	ibmdb.open('DRIVER={DB2};DATABASE=' + db + ';HOSTNAME=' + host + ';UID=' + user + ';PWD=' + password + ';PORT=' + (port || 50001) + ';PROTOCOL=TCPIP', function (err,conn) {
		if (err) return cb(err);
		conn.close(function () {
			console.log('done');
			return cb();
		});
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



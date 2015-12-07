var MongoClient = require('mongodb').MongoClient;
var async = require('async');

var executeQueries = function(host, port, user, password, db, queries, options, cb) {
	
	var url = 'mongodb://' + ((user && password) ? user + ':' + password + '@' : '') + host + ':' + (port || 27017) + '/' + db;
	MongoClient.connect(url, function(err, db) {
	    if(err) return cb(err);

	    var methodCheck = '.aggregate(';
		var result = [];

		async.eachSeries(queries, function(query, callback) {



			if(query.indexOf(methodCheck) == -1){
				result.push(new Error('Only aggregates are supported. Send a query inn this form: myCollection.aggregate([ ...aggregation stages... ])'));
				return callback();
			}

			var collection = query.substr(0, query.indexOf(methodCheck));

			if(!collection){
				result.push(new Error('No collection has been sent'));
				return callback();
			}

			query = query.substr((query.indexOf(methodCheck) + methodCheck.length), (query.length - (query.indexOf(methodCheck) + methodCheck.length) - 1));

			try{
				JSON.parse(query)
			}catch(e){
				result.push(new Error(e));
				return callback();
			}
		    
		    db.collection(collection).aggregate(
		      	JSON.parse(query)
		    ).toArray(function(err, output) {

				if (err) 
					result.push(new Error(err));
				else 
					result.push(formatOutput(output));



				return callback();
		    });
		}, function() {
			db.close();
			console.log(result);
			return cb(null, result);
		});

	});
};

var testConnection = function(host, port, user, password, db, options, cb) {
	var url = 'mongodb://' + ((user && password) ? user + ':' + password + '@' : '') + host + ':' + (port || 27017) + '/' + db;
	MongoClient.connect(url, function(err, db) {
	    if(err) return cb(err);
	    console.log("Connected correctly to server");
		db.close();
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
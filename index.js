var dbList = {
	sqlServer:{
		name: 	'Microsoft SQL Server',
		script: require('./db/sqlServer')
	},
	oracle:{
		name: 	'Oracle',
		script: require('./db/oracle')
	},
	mysql:{
		name: 	'MySQL',
		script: require('./db/mysql')
	},
	postgreSql:{
		name: 	'PostgreSQL',
		script: require('./db/postgreSql')
	},
	db2:{
		name: 	'IBM DB2',
		script: require('./db/db2')
	},
	mariadb:{
		name: 	'MariaDB',
		script: require('./db/mariadb')
	},
	mongodb:{
		name: 	'MongoDB',
		script: require('./db/mongodb')
	},
	sybase:{
		name: 	'SAP ASE / Sybase',
		script: require('./db/sybase')
	}
};

var executeQueries = function(type, host, port, user, password, db, queries, options, cb){
	if(typeof options === 'function') cb = [options, options = cb][0];
	if(Object.keys(dbList).indexOf(type) == -1) return cb('Unknown Database type');
	if(!host || !db || !queries) return cb('Insufficient parameters');

	if(typeof queries === 'string'){
		var singleQuery = true;
		queries = [queries];
	}

	dbList[type].script.executeQueries(host, port, user, password, db, queries, options, function(err, data){
		if(err) return cb(err);
		if(singleQuery) data = data[0];
		if(data instanceof Error) return cb(data);

		return cb(null, data);
	});
};

var testConnection = function(type, host, port, user, password, db, options, cb){
	if(typeof options === 'function') cb = [options, options = cb][0];
	if(Object.keys(dbList).indexOf(type) == -1) return cb('Unknown Database type');
	if(!host || !db) return cb('Insufficient parameters');

	dbList[type].script.testConnection(host, port, user, password, db, options, function(err, data){
		if(err) return cb(err);
		return cb();
	});
};

var getConnectionTypes = function (){
	var types = [];
	for (dbName in dbList){
		types.push({key: dbName, name: dbList[dbName].name});
	}
	return types;
};

module.exports = {
	executeQueries: executeQueries,
	testConnection: testConnection,
	getConnectionTypes: getConnectionTypes
};
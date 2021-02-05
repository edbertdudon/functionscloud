//
//  connector.js
//  Tart
//
//  Created by Edbert Dudon on 7/8/19.
//  Copyright © 2019 Project Tart. All rights reserved.
//
//  Deploy:
//  gcloud functions deploy canceljob --runtime nodejs10 --trigger-http --allow-unauthenticated --timeout=540s
//
//	mssql
//	tutorial: https://adamwilbert.com/blog/2018/3/26/get-started-with-sql-server-on-macos-complete-with-a-native-gui
//	hostname: localhost,1401
//	SQL login
//	username: SA
//	password: MyNewPass!
//
//	Docker:
//	list: docker ps -a
//	create: sudo docker run -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=YourStrong!Passw0rd' -p 1401:1433 --name sqlserver1 -d microsoft/mssql-server-linux:2017-latest
//	docker start sqlserver1
//	docker stop sqlserver1
//
const express = require('express')
const cors = require('cors')
// const cors = require('cors')({origin: '*'});
// var cors = require('cors')({origin: 'https://www.tartcl.com'});
var admin = require('firebase-admin');
admin.initializeApp({
	credential: admin.credential.cert('./tart-90ca2-firebase-adminsdk-kf272-41cfb98e9e.json'),
	databaseURL: 'https://tart-90ca2.firebaseio.com'
})
const mysql = require('mysql')
const mssql = require('mssql')
const oracledb = require('oracledb');
oracledb.initOracleClient({libDir: './lib/instantclient_19_3'})
const fs = require('fs');

const app = express()

app.use(cors({ origin: true }))

// app.use((req, res, next) => {
// 	res.header("Access-Control-Allow-Origin", '*');
// 	// res.header("Access-Control-Allow-Origin", 'https://www.tartcl.com');
// 	res.header("Acces-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
// 	next();
// })

const authenticateEndUser = async (req, res, next) => {
	// console.log('Check if request is authorized with Firebase ID token');

	if ((!req.headers.authorization || !req.headers.authorization.startsWith('Bearer ')) &&
		!(req.cookies && req.cookies.__session)) {
		console.error('No Firebase ID token was passed as a Bearer token in the Authorization header.',
			'Make sure you authorize your request by providing the following HTTP header:',
			'Authorization: Bearer <Firebase ID Token>',
			'or by passing a "__session" cookie.');
		res.status(403).send('Unauthorized');
		return;
	}

	let idToken;
	if (req.headers.authorization && req.headers.authorization.startsWith('Bearer ')) {
		// console.log('Found "Authorization" header');
		// Read the ID Token from the Authorization header.
		idToken = req.headers.authorization.split('Bearer ')[1];
	} else if(req.cookies) {
		// console.log('Found "__session" cookie');
		// Read the ID Token from cookie.
		idToken = req.cookies.__session;
	} else {
		// No cookie
		res.status(403).send('Unauthorized');
		return;
	}

	try {
		const decodedIdToken = await admin.auth().verifyIdToken(idToken);
		// console.log('ID Token correctly decoded');
		req.user = decodedIdToken;
		next();
	} catch (error) {
		console.error('Error while verifying Firebase ID token:', error);
		res.status(403).send('Unauthorized');
		return;
	}
}

app.use(authenticateEndUser)

app.use(express.json())

app.listen(8000)

async function encryptAsymmetric(client, versionName, plaintextBuffer) {
	const [publicKey] = await client.getPublicKey({
		name: versionName,
	});

	const crypto = require('crypto');

	const ciphertextBuffer = crypto.publicEncrypt({
		key: publicKey.pem,
		oaepHash: 'sha256',
		padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
	},	plaintextBuffer);

	console.log(`Ciphertext: ${ciphertextBuffer.toString('base64')}`);
	return ciphertextBuffer;
}

async function saveCredentials(firestore, credentials, uid, host, port) {
	const document = firestore.collection('connections').doc(uid);
	const doc = await document.get()

	if (doc.exists) {
		document.update({ [host + ":" + port]: credentials })
	} else {
		document.set({ [host + ":" + port]: credentials })
	}

	console.log('Saved credentials');
}

// *** MySQL ***

app.post('/connectMySql', async (req, res) => {
	const con = mysql.createConnection({
		host: req.body.host,
		user: req.body.user,
		password: req.body.password,
		port: req.body.port,
	})

	con.connect(async err => {
		if (err) res.json({status: 'ERROR'})
		res.json({status: 'CONNECTED'})

		console.log('Connected.')

		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const userId = req.body.uid
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';
		const plaintextBuffer = Buffer.from(req.body.password);

		const {KeyManagementServiceClient} = require('@google-cloud/kms');
		const client = new KeyManagementServiceClient();
		const versionName = client.cryptoKeyVersionPath(
			projectId,
			region,
			keyRingId,
			keyId,
			versionId
		);

		const encryptedPassword = await encryptAsymmetric(client, versionName, plaintextBuffer)

		const Firestore = require('@google-cloud/firestore');

		const firestore = new Firestore({
			projectId: projectId,
			keyFilename: './tart-90ca2-9d37c42ef480.json',
		});

		const credentials = {
			host: req.body.host,
			user: req.body.user,
			password: encryptedPassword,
			port: req.body.port,
			connector: req.body.connector,
		}

		await saveCredentials(firestore, credentials, userId, req.body.host, req.body.port)
	})

	con.end()
})

app.post('/listDatabasesMySql', (req, res) => {
	const con = mysql.createConnection({
		host: req.body.host,
		user: req.body.user,
		password: req.body.password,
		port: req.body.port,
	})

	const SQL_STATEMENT = "SHOW DATABASES"

	con.query(SQL_STATEMENT, (err, result, fields) => {
		if (err) res.json({status: 'ERROR'});

		const databases = result.map(database => database.Database)
		res.json(databases)
	})

	con.end()
})

app.post('/listTablesMySql', (req, res) => {
	const con = mysql.createConnection({
		host: req.body.host,
		user: req.body.user,
		password: req.body.password,
		database: req.body.database,
		port: req.body.port,
	})

	const SQL_STATEMENT = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + req.body.database + "'"

	con.query(SQL_STATEMENT, (err, result, fields) => {
		if (err) res.json({status: 'ERROR'});

		const tables = result.map(table => table.TABLE_NAME)
		res.json(tables)
	})

	con.end()
})

app.post('/getTableSampleMySql', (req,res) => {
	const con = mysql.createConnection({
		host: req.body.host,
		user: req.body.user,
		password: req.body.password,
		database: req.body.database,
		port: req.body.port,
	})

	const SQL_STATEMENT = "SELECT * FROM " + req.body.table + " LIMIT 200"

	con.query(SQL_STATEMENT, (err, result, fields) => {
		if (err) res.json({status: 'ERROR'});
		res.json(result)
	})

	con.end()
})

// *** SQL Server ***

app.post('/connectMsSql', (req, res) => {
	const config = {
		server: req.body.host,
		user: req.body.user,
		password: req.body.password,
		port: parseInt(req.body.port),
	};

	mssql.connect(config, function(err) {
		if (err) {
			res.json({status: 'ERROR'})
		} else {
			console.log('Connected.')
			res.json({status: 'CONNECTED'})
		}
	});
})

app.post('/listDatabasesMsSql', (req, res) => {
	const config = {
		server: req.body.host,
		user: req.body.user,
		password: req.body.password,
		port: parseInt(req.body.port),
	};

	const SQL_STATEMENT = "SELECT name FROM master.sys.databases"

	mssql.connect(config, function(err) {
		if (err) {
			res.json({status: 'ERROR'})
		} else {
			let request = new mssql.Request();
			request.query(SQL_STATEMENT, function(err, result) {
				if (err) {
					res.json({status: 'ERROR'});
				} else {
					let databases = result.recordset.map(database => database.name)
					res.json(databases)
				}
			});
		}
	});
})

app.post('/listTablesMsSql', (req, res) => {
	const config = {
		server: req.body.host,
		user: req.body.user,
		password: req.body.password,
		port: parseInt(req.body.port),
		database: req.body.database,
	};

	const SQL_STATEMENT = "Select name AS '" + req.body.database + "' From sys.tables"

	mssql.connect(config, function(err) {
		if (err) {
			res.json({status: 'ERROR'})
		} else {
			let request = new mssql.Request();
			request.query(SQL_STATEMENT, function(err, result) {
				if (err) {
					res.json({status: 'ERROR'});
				} else {
					let tables = result.recordset.map(table => table[req.body.database])
					res.json(tables)
				}
			});
		}
	});
})

app.post('/getTableSampleMsSql', (req, res) => {
	const config = {
		server: req.body.host,
		user: req.body.user,
		password: req.body.password,
		port: parseInt(req.body.port),
		database: req.body.database,
	};

	const SQL_STATEMENT = "SELECT TOP 200 * FROM " + req.body.table

	mssql.connect(config, function(err) {
		if (err) {
			res.json({status: 'ERROR'})
		} else {
			let request = new mssql.Request();
			request.query(SQL_STATEMENT, function(err, result) {
				if (err) {
					res.json({status: 'ERROR'});
				} else {
					console.log(result.recordset)
					res.json(result.recordset)
				}
			});
		}
	});
})

// *** Oracle DB ***

app.post('/connectOracledb', async (req, res) => {
	let connection;
	const config = {
		user: req.body.user,
		password: req.body.password,
		connectString: "(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST =" + req.body.host + ")(PORT =" + req.body.port + "))(CONNECT_DATA =(SID=" + req.body.sid + ")))"
	};

	try {
		connection = await oracledb.getConnection(config);

		console.log('Connected.')
		res.json({status: 'CONNECTED'})
	} catch(err) {
		res.json({status: 'ERROR'});
	} finally {
		if (connection) {
			try {
				await connection.close();
			} catch (err) {
				res.json({status: 'ERROR'});
			}
		}
	}
})

// app.post('/listDatabasesOracledb', async (req, res) => {
// 	const oracledb = require('oracledb');
// 	let connection;
// 	const config = {
// 		user: req.body.user,
// 		password: req.body.password,
// 		connectString: "(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST =" + req.body.host + ")(PORT =" + req.body.port + "))(CONNECT_DATA =(SID=" + req.body.sid + ")))"
// 	};
//
// 	try {
// 		oracledb.initOracleClient({libDir: './lib/instantclient_19_3'})
// 		connection = await oracledb.getConnection(config);
//
// 		const SQL_STATEMENT = "select * from v$database"
// 		const result = await connection.execute(SQL_STATEMENT, [], {});
//
//
// 		res.json(result.rows)
// 	} catch(err) {
// 		console.error(err)
// 		res.json({status: 'ERROR', error: err});
// 	} finally {
// 		if (connection) {
// 			try {
// 				await connection.close();
// 			} catch (err) {
// 				console.error(err);
// 			}
// 		}
// 	}
// })

app.post('/listTablesOracledb', async (req, res) => {
	let connection;
	const config = {
		user: req.body.user,
		password: req.body.password,
		connectString: "(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST =" + req.body.host + ")(PORT =" + req.body.port + "))(CONNECT_DATA =(SID=" + req.body.sid + ")))"
	};

	try {
		connection = await oracledb.getConnection(config);

		const SQL_STATEMENT = "SELECT table_name FROM user_tables ORDER BY table_name"
		const result = await connection.execute(SQL_STATEMENT, [], {});

		const tables = result.rows.map(table => table[0])
		res.json(tables)
	} catch(err) {
		res.json({status: 'ERROR'});
	} finally {
		if (connection) {
			try {
				await connection.close(err => {
					if (err) console.log(err)
				})
			} catch (err) {
				console.log(err)
				res.json({status: 'ERROR'});
			}
		}
	}
})

app.post('/getTableSampleOracledb', async (req, res) => {
	let connection;
	const config = {
		user: req.body.user,
		password: req.body.password,
		connectString: "(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST =" + req.body.host + ")(PORT =" + req.body.port + "))(CONNECT_DATA =(SID=" + req.body.sid + ")))"
	};

	try {
		connection = await oracledb.getConnection(config);

		const SQL_STATEMENT = `SELECT * FROM ` + req.body.table
		const result = await connection.execute(SQL_STATEMENT, [], {maxRows: 200});
		const sample = {headers: result.metaData, rows:result.rows}

		res.json(sample)
	} catch(err) {
		res.json({status: 'ERROR'});
	} finally {
		if (connection) {
			try {
				await connection.close();
			} catch (err) {
				res.json({status: 'ERROR'});
			}
		}
	}
})

async function decryptAsymmetric(client, versionName, ciphertext) {
  const [result] = await client.asymmetricDecrypt({
	name: versionName,
	ciphertext: ciphertext,
  });

  const plaintext = result.plaintext.toString('utf8');

  console.log(`Plaintext: ${plaintext}`);
  return plaintext;
}

app.post('/decryptAsymmetricR', async (req,res) => {
	const projectId = 'tart-90ca2';
	const region = 'us-central1';
	const keyRingId = 'cloudR-user-database';
	const keyId = 'database-login';
	const versionId = '1';
	const userId = req.body.uid

	const Firestore = require('@google-cloud/firestore');

	const firestore = new Firestore({
		projectId: projectId,
		keyFilename: './tart-90ca2-9d37c42ef480.json',
	});

	const credentials = await getCredentials(firestore, userId)

	const ciphertext = Buffer.from(credentials.password);

	const {KeyManagementServiceClient} = require('@google-cloud/kms');
	const client = new KeyManagementServiceClient();

	const versionName = client.cryptoKeyVersionPath(
		projectId,
		region,
		keyRingId,
		keyId,
		versionId
	);

	return decryptAsymmetric();
})

module.export = {app}

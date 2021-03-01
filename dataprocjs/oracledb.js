//
//  Oracle DB connector
//  Tart
//
//  Created by Edbert Dudon on 7/8/19.
//  Copyright © 2019 Project Tart. All rights reserved.
//
//	Local testing:
//
//
const cors = require('cors')({origin: '*'});
const constants = require('./core/options')
const admin = require('./index');
const kms = require('./core/kms');

exports.connectOracledb = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const host = req.body.host;
		const user = req.body.user;
		const password = req.body.password;
		const port = req.body.port;
		const sid = req.body.sid;
		const connector = req.body.connector;
		const ssl = req.body.ssl;
		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const userId = req.body.uid;
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';

		const oracledb = require('oracledb');
		try {
			oracledb.initOracleClient({libDir: '/Users/eb/Downloads/instantclient_19_8'});
	  } catch (err) {
	    console.error(err);
	  }

		const config = {
			user: user,
			password: password,
			externalAuth: ssl,
			connectString: `${host}:${port}/${sid}`
		};

		let conn;
		try {
			conn = await oracledb.getConnection(config);

			res.json({status: 'CONNECTED'})

			console.log('Connected.')

			const plaintextBuffer = Buffer.from(password);

			const {KeyManagementServiceClient} = require('@google-cloud/kms');

			const client = new KeyManagementServiceClient({
				keyFilename: './tart-90ca2-e8da41e06847.json',
			});

			const versionName = client.cryptoKeyVersionPath(
				projectId,
				region,
				keyRingId,
				keyId,
				versionId
			);

			const encryptedPassword = await kms.encryptAsymmetric(
				client,
				versionName,
				plaintextBuffer
			);

			console.log('Encrypted Password.');

			const Firestore = require('@google-cloud/firestore');

			const firestore = new Firestore({
				projectId: projectId,
				keyFilename: './tart-90ca2-e8da41e06847.json',
			});

			const credentials = {
				host: host,
				user: user,
				password: encryptedPassword,
				port: port,
				sid: sid,
				connector: connector,
				ssl: ssl,
			}

			await kms.saveCredentials(firestore, credentials, userId, host, port)
		} catch(err) {
			console.log(err)
			res.status(403).send(err);
		} finally {
			if (conn) {
				try {
					await conn.close();
				} catch (err) {
					res.status(403).send(err);
				}
			}
		}
	})
}

// exports.listDatabasesOracledb = async (req, res) => {
// 	cors(req, res, async () => {
		// const isAuthenticated = admin.authenticate(req, res);
		// if (!isAuthenticated) return;
//
// 		const connection = req.body.connection
// 		const userId = req.body.uid
// 		const projectId = 'tart-90ca2';
// 		const region = 'us-central1';
// 		const keyRingId = 'cloudR-user-database';
// 		const keyId = 'database-login';
// 		const versionId = '1';
//
// 		const Firestore = require('@google-cloud/firestore');
//
// 		const firestore = new Firestore({
// 			projectId: projectId,
// 			keyFilename: './tart-90ca2-e8da41e06847.json',
// 		});
//
// 		const connections = await kms.getConnectionDocument(firestore, userId)
//
// 		const credentials = connections[connection]
// 		const host = credentials.host;
// 		const user = credentials.user;
// 		const encryptedPassword = credentials.password;
// 		const port = credentials.port;
// 		const sid = credentials.sid;
// 		const ssl = credentials.ssl;
// 		const ciphertext = Buffer.from(encryptedPassword);
//
// 		const {KeyManagementServiceClient} = require('@google-cloud/kms');
//
// 		const client = new KeyManagementServiceClient({
// 			keyFilename: './tart-90ca2-e8da41e06847.json',
// 		});
//
// 		const password = await kms.decryptAsymmetric(
// 			client,
// 			ciphertext,
// 			projectId,
// 			region,
// 			keyRingId,
// 			keyId,
// 			versionId
// 		);
//
// 		const oracledb = require('oracledb');
// 		try {
// 			oracledb.initOracleClient({libDir: '/Users/eb/Downloads/instantclient_19_8'});
// 	  } catch (err) {
// 	    console.error(err);
// 	  }
//
// 		const config = {
// 			user: user,
// 			password: password,
// 			externalAuth: ssl,
// 			connectString: `${host}:${port}/${sid}`
// 		};
//
// 		let conn;
// 		try {
// 			conn = await oracledb.getConnection(config);
//
// 			const SQL_STATEMENT = "select * from v$database"
// 			const result = await conn.execute(SQL_STATEMENT, [], {});
// 		} catch(err) {
// 			res.status(403).send(err);
// 		} finally {
// 			if (conn) {
// 				try {
// 					await conn.close();
// 				} catch (err) {
// 					res.status(403).send(err);
// 				}
// 			}
// 		}
// 	})
// }

exports.listTablesOracledb = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const connection = req.body.connection
		const database = req.body.database;
		const userId = req.body.uid
		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';

		const Firestore = require('@google-cloud/firestore');

		const firestore = new Firestore({
			projectId: projectId,
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const connections = await kms.getConnectionDocument(firestore, userId)

		const credentials = connections[connection]

		const host = credentials.host;
		const user = credentials.user;
		const encryptedPassword = credentials.password;
		const port = credentials.port;
		const sid = credentials.sid;
		const ssl = credentials.ssl;
		const ciphertext = Buffer.from(encryptedPassword);

		const {KeyManagementServiceClient} = require('@google-cloud/kms');

		const client = new KeyManagementServiceClient({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const password = await kms.decryptAsymmetric(
			client,
			ciphertext,
			projectId,
			region,
			keyRingId,
			keyId,
			versionId
		);

		const oracledb = require('oracledb');
		try {
			oracledb.initOracleClient({libDir: '/Users/eb/Downloads/instantclient_19_8'});
	  } catch (err) {
	    console.error(err);
	  }

		const config = {
			user: user,
			password: password,
			externalAuth: ssl,
			connectString: `${host}:${port}/${sid}`
		};

		let conn;
		try {
			conn = await oracledb.getConnection(config);

			const SQL_STATEMENT = "SELECT table_name FROM user_tables ORDER BY table_name"

			const result = await conn.execute(SQL_STATEMENT);

			const tables = result.rows.map(table => table[0])

			res.json(tables)
		} catch(err) {
			res.status(403).send(err);
		} finally {
			if (conn) {
				try {
					await conn.close();
				} catch (err) {
					res.status(403).send(err);
				}
			}
		}
	});
}

exports.getTableSampleOracledb = async (req,res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const connection = req.body.connection
		const database = req.body.database;
		const table = req.body.table;
		const userId = req.body.uid
		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';

		const Firestore = require('@google-cloud/firestore');

		const firestore = new Firestore({
			projectId: projectId,
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const connections = await kms.getConnectionDocument(firestore, userId)

		const credentials = connections[connection]
		const host = credentials.host;
		const user = credentials.user;
		const encryptedPassword = credentials.password;
		const port = credentials.port;
		const sid = credentials.sid;
		const ssl = credentials.ssl;
		const ciphertext = Buffer.from(encryptedPassword);

		const {KeyManagementServiceClient} = require('@google-cloud/kms');

		const client = new KeyManagementServiceClient({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const password = await kms.decryptAsymmetric(
			client,
			ciphertext,
			projectId,
			region,
			keyRingId,
			keyId,
			versionId
		);

		const oracledb = require('oracledb');
		try {
			oracledb.initOracleClient({libDir: '/Users/eb/Downloads/instantclient_19_8'});
	  } catch (err) {
	    console.error(err);
	  }

		const config = {
			user: user,
			password: password,
			externalAuth: ssl,
			connectString: `${host}:${port}/${sid}`
		};

		let conn;
		try {
			conn = await oracledb.getConnection(config);

			const SQL_STATEMENT = `SELECT * FROM ${table}`

			const result = await conn.execute(SQL_STATEMENT, [], {maxRows: constants.maxRows});
			console.log(result.rows)
			const sample = { headers: result.metaData, rows: result.rows }

			res.json(sample)
		} catch(err) {
			res.status(403).send(err);
		} finally {
			if (conn) {
				try {
					await conn.close();
				} catch (err) {
					res.status(403).send(err);
				}
			}
		}
	})
}

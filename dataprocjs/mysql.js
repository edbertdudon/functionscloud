//
//  MySQL connector
//  Tart
//
//  Created by Edbert Dudon on 7/8/19.
//  Copyright © 2019 Project Tart. All rights reserved.
//
//	Local testing:
//	localhost:3306 MyNewPass
//
const cors = require('cors')({origin: '*'});
const constants = require('./core/options')
const admin = require('./index');
const kms = require('./core/kms');

exports.connectMySql = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const host = req.body.host;
		const user = req.body.user;
		const password = req.body.password;
		const port = req.body.port;
		const ssl = req.body.ssl;
		const connector = req.body.connector;
		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const userId = req.body.uid;
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';

		const mysql = require('mysql');

		const con = mysql.createConnection({
			host: host,
			user: user,
			password: password,
			port: port,
		})

		con.connect(async err => {
			if (err) {
				res.status(403).send(err);
				return
			}
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
				connector: connector,
				ssl: ssl,
			}

			await kms.saveCredentials(firestore, credentials, userId, host, port)
		})

		con.end()
	})
}

exports.listDatabasesMySql = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const connection = req.body.connection
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

		const mysql = require('mysql')

		const con = mysql.createConnection({
			host: host,
			user: user,
			password: password,
			port: port,
		})

		const SQL_STATEMENT = "SHOW DATABASES"

		con.query(SQL_STATEMENT, (err, result, fields) => {
			if (err) {
				res.status(403).send(err);
				return
			}

			const databases = result.map(database => database.Database)
			res.json(databases)
		})

		con.end()
	})
}

exports.listTablesMySql = async (req, res) => {
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

		const mysql = require('mysql')

		const con = mysql.createConnection({
			host: host,
			user: user,
			password: password,
			port: port,
		})


		const SQL_STATEMENT = `SELECT table_name FROM information_schema.tables WHERE table_schema = '${database}'`

		con.query(SQL_STATEMENT, (err, result, fields) => {
			if (err) {
				res.status(403).send(err);
				return
			}

			const tables = result.map(table => table.TABLE_NAME)
			res.json(tables)
		})

		con.end()
	});
}

exports.getTableSampleMySql = async (req,res) => {
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

		const mysql = require('mysql')

		const con = mysql.createConnection({
			host: host,
			user: user,
			password: password,
			port: port,
		})


		const SQL_STATEMENT = `SELECT * FROM ${table} LIMIT ${constants.maxRows}`

		con.query(SQL_STATEMENT, (err, result, fields) => {
			if (err) {
				res.status(403).send(err);
				return
			}

			res.json(result)
		})

		con.end()
	})
}

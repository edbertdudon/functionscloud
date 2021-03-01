//
//  SQL Server connector
//  Tart
//
//  Created by Edbert Dudon on 7/8/19.
//  Copyright © 2019 Project Tart. All rights reserved.
//
//	Local testing:
//	docker run -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=MyNewPass!' -e
//		'MSSQL_PID=Express' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest-ubuntu
//
const cors = require('cors')({origin: '*'});
const constants = require('./core/options')
const admin = require('./index');
const kms = require('./core/kms');

exports.connectMsSql = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const host = req.body.host;
		const user = req.body.user;
		const password = req.body.password;
		const port = req.body.port;
		const connector = req.body.connector;
		const ssl = req.body.ssl;
		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const userId = req.body.uid;
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';

		const mssql = require('mssql')

		const config = {
			server: host,
			user: user,
			password: password,
			port: parseInt(port),
			options: {
				encrypt: ssl,
			}
		};

		mssql.connect(config).then(async pool => {
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
		}).catch((err) => {
			res.status(403).send(err);
		})
	})
}

exports.listDatabasesMsSql = async (req, res) => {
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

		const mssql = require('mssql')

		const config = {
			server: host,
			user: user,
			password: password,
			port: parseInt(port),
			options: {
				encrypt: ssl,
			}
		};

		const SQL_STATEMENT = "SELECT name FROM master.sys.databases EXEC sp_databases"

		mssql.connect(config, function(err) {
			if (err) {
				res.status(403).send(err);
				return
			}

			let request = new mssql.Request();
			request.query(SQL_STATEMENT, function(err, result) {
				if (err) {
					res.status(403).send(err);
					return
				}

				let databases = result.recordset.map(database => database.name)
				res.json(databases)
			});
		})
	})
}

exports.listTablesMsSql = async (req, res) => {
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

		const mssql = require('mssql')

		const config = {
			server: host,
			user: user,
			password: password,
			port: parseInt(port),
			database: database,
			options: {
				encrypt: ssl,
			}
		};

		const SQL_STATEMENT = `Select name AS '${database}' From sys.tables`

		mssql.connect(config, function(err) {
			if (err) {
				res.status(403).send(err);
				return
			}

			let request = new mssql.Request();
			request.query(SQL_STATEMENT, function(err, result) {
				if (err) {
					res.status(403).send(err);
					return
				}

				let tables = result.recordset.map(table => table[database])
				res.json(tables)
			});
		})
	});
}

exports.getTableSampleMsSql = async (req,res) => {
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

		const mssql = require('mssql')

		const config = {
			server: host,
			user: user,
			password: password,
			port: parseInt(port),
			database: database,
			options: {
				encrypt: ssl,
			}
		};

		const SQL_STATEMENT = `SELECT TOP ${constants.maxRows} * FROM ${table}`

		mssql.connect(config, function(err) {
			if (err) {
				res.status(403).send(err);
				return
			}

			let request = new mssql.Request();
			request.query(SQL_STATEMENT, function(err, result) {
				if (err) {
					res.status(403).send(err);
					return
				}

				res.json(result.recordset)
			});
		})
	})
}

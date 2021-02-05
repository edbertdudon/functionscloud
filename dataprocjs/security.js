//
//  security.js
//  Tart
//
//  Created by Edbert Dudon on 7/8/19.
//  Copyright © 2019 Project Tart. All rights reserved.
//
const cors = require('cors')({origin: 'https://www.tartcl.com'});

async function createAndAccessSecret(parent, secretId, payload) {
	// Create the secret with automation replication.
	const [secret] = await client.createSecret({
		parent: parent,
		secret: {
			name: secretId,
			replication: {
				automatic: {},
			},
		},
		secretId,
	});

	console.info(`Created secret ${secret.name}`);

	// Add a version with a payload onto the secret.
	const [version] = await client.addSecretVersion({
		parent: secret.name,
		payload: {
			data: Buffer.from(payload, 'utf8'),
		},
	});

	console.info(`Added secret version ${version.name}`);

	// Access the secret.
	const [accessResponse] = await client.accessSecretVersion({
		name: version.name,
	});

	const responsePayload = accessResponse.payload.data.toString('utf8');
	console.info(`Payload: ${responsePayload}`);
}

module.exports = {
	createAndAccessSecret: async (req,res) => {
		cors(req, res, async () => {
			const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
			const client = new SecretManagerServiceClient();
			const parent = 'projects/tart'
			const secretId = req.body.secretId
			const payload = req.body.payload

			createAndAccessSecret()
		})
	}
}

exports.encryptAsymmetric = async (client, versionName, plaintextBuffer) => {
	const [publicKey] = await client.getPublicKey({
		name: versionName,
	});

	const crc32c = require('fast-crc32c');
	if (publicKey.name !== versionName) {
		throw new Error('GetPublicKey: request corrupted in-transit');
	}
	if (crc32c.calculate(publicKey.pem) !== Number(publicKey.pemCrc32c.value)) {
		throw new Error('GetPublicKey: response corrupted in-transit');
	}

	const crypto = require('crypto');

	const ciphertextBuffer = crypto.publicEncrypt(
		{
			key: publicKey.pem,
			oaepHash: 'sha256',
			padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
		},
		plaintextBuffer
	);

	// console.log(`Ciphertext: ${ciphertextBuffer.toString('base64')}`);
	return ciphertextBuffer;
}

exports.decryptAsymmetric = async (
	client, ciphertext, projectId, region, keyRingId, keyId, versionId
) => {
	const versionName = client.cryptoKeyVersionPath(
		projectId,
		region,
		keyRingId,
		keyId,
		versionId
	);

	const crc32c = require('fast-crc32c');
	const ciphertextCrc32c = crc32c.calculate(ciphertext);

	const [decryptResponse] = await client.asymmetricDecrypt({
		name: versionName,
		ciphertext: ciphertext,
		ciphertextCrc32c: {
			value: ciphertextCrc32c,
		},
	});

	if (!decryptResponse.verifiedCiphertextCrc32c) {
		throw new Error('AsymmetricDecrypt: request corrupted in-transit');
	}
	if (
		crc32c.calculate(decryptResponse.plaintext) !==
		Number(decryptResponse.plaintextCrc32c.value)
	) {
		throw new Error('AsymmetricDecrypt: response corrupted in-transit');
	}

	console.log('Decrypt succesful.')

	const plaintext = decryptResponse.plaintext.toString('utf8');

	// console.log(`Plaintext: ${plaintext}`);
	return plaintext;
}

exports.getConnectionDocument = async (firestore, uid) => {
	const document = firestore.collection('connections').doc(uid);
	const doc = await document.get();

	return doc.data();
}

exports.saveCredentials = async (firestore, credentials, userId, host, port) => {
	const document = firestore.collection('connections').doc(userId);

	const doc = await document.get()

	if (doc.exists) {
		document.update({ [host + ":" + port]: credentials })
	} else {
		document.set({ [host + ":" + port]: credentials })
	}

	console.log('Saved credentials');
}

// exports.decryptAsymmetricR = async (req, res) => {
// 	cors(req, res, async () => {
// 		const projectId = 'tart-90ca2';
// 		const region = 'us-central1';
// 		const keyRingId = 'cloudR-user-database';
// 		const keyId = 'database-login';
// 		const versionId = '1';
// 		const userId = req.body.uid
//
// 		const Firestore = require('@google-cloud/firestore');
// 		const firestore = new Firestore({
// 			projectId: projectId,
// 			keyFilename: './tart-90ca2-9d37c42ef480.json',
// 		});
//
// 		const credentials = await getCredentials(firestore, userId)
//
// 		// credentials = credentials[localhost:3306]
//
// 		const ciphertext = Buffer.from(credentials.password);
//
// 		const {KeyManagementServiceClient} = require('@google-cloud/kms');
// 		const client = new KeyManagementServiceClient();
//
// 		const versionName = client.cryptoKeyVersionPath(
// 			projectId,
// 			region,
// 			keyRingId,
// 			keyId,
// 			versionId
// 		);
//
// 		return decryptAsymmetric();
// 	})
// }

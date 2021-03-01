const cors = require('cors')({origin: '*'});
const admin = require('./index');

// exports.copysample = async (req, res) => {
//   cors(req, res, async () => {
		// const isAuthenticated = admin.authenticate(req, res);
		// if (!isAuthenticated) return;
//
//     const {Storage} = require('@google-cloud/storage');
//
//     const storage = new Storage({
// 			keyFilename: './tart-90ca2-e8da41e06847.json',
// 		});
//
//     const userId = req.body.uid
//     const destFilename = 'user/' + userId + '/pit pd.csv'
//
//     await storage
//       .bucket('tart-90ca2.appspot.com')
//       .file('scripts/pit pd.csv')
//       .copy(storage.bucket('tart-90ca2.appspot.com').file(destFilename))
//   })
// }

async function moveFile(storage, bucketName, srcFilename, destFilename) {
	await storage.bucket(bucketName).file(srcFilename).move(destFilename);

	console.log(
		`gs://${bucketName}/${srcFilename} moved to gs://${bucketName}/${destFilename}.`
	);
}

async function listFilenamesByPrefix(storage, prefix, bucketName) {
	const options = {
		prefix: prefix,
	};

	// if (delimiter) {
	//   options.delimiter = delimiter;
	// }

	const [files] = await storage.bucket(bucketName).getFiles(options);

	return files.map((file) => file.name)
}

async function addSuffixIfNameClash(storage, prefix, bucketName, destFilename) {
	const files = await listFilenamesByPrefix(storage, prefix, bucketName)

	if (files.includes(destFilename)) {
		const currentDate = new Date().toLocaleTimeString();
		destFilename = `${destFilename} ${currentDate.replace(/:/g, '.')}`
	}

	return destFilename
}

exports.moveToTrash = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const bucketName = 'tart-90ca2.appspot.com';
		const userId = req.body.uid;
		const srcFilename = `user/${userId}/worksheets/${req.body.filename}`;
		let destFilename = `user/${userId}/trash/${req.body.filename}`;
		const prefix = `user/${userId}/trash`;

		const {Storage} = require('@google-cloud/storage');

		const storage = new Storage({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		destFilename = addSuffixIfNameClash(storage, prefix, bucketName, destFilename)

		moveFile(
			storage,
			bucketName,
			srcFilename,
			destFilename
		);
	});
}

exports.moveToWorksheets = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const bucketName = 'tart-90ca2.appspot.com';
		const userId = req.body.uid;
		const srcFilename = `user/${userId}/trash/${req.body.filename}`;
		let destFilename = `user/${userId}/worksheets/${req.body.filename}`;
		const prefix = `user/${userId}/worksheets`;

		const {Storage} = require('@google-cloud/storage');

		const storage = new Storage({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		destFilename = addSuffixIfNameClash(storage, prefix, bucketName, destFilename)

		moveFile(
			storage,
			bucketName,
			srcFilename,
			destFilename
		);
	});
}

exports.renameWorksheet = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const bucketName = 'tart-90ca2.appspot.com';
		const userId = req.body.uid;
		const srcFilename = `user/${userId}/worksheets/${req.body.src}`;
		const destFilename = `user/${userId}/worksheets/${req.body.dest}`;

		const {Storage} = require('@google-cloud/storage');

		const storage = new Storage({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		moveFile(
			storage,
			bucketName,
			srcFilename,
			destFilename
		);
	});
}

async function queryAndFilterEmail(db, email) {
	const usersRef = db.collection('users');

	const allEmailsRes = await usersRef.where('email', '==', email).get();

	return allEmailsRes;
}

async function copyFile(storage, srcBucketName, srcFilename, destBucketName, destFilename) {
	await storage
		.bucket(srcBucketName)
		.file(srcFilename)
		.copy(storage.bucket(destBucketName).file(destFilename));

	console.log(
		`gs://${srcBucketName}/${srcFilename} copied to gs://${destBucketName}/${destFilename}.`
	);
}

exports.shareFile = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const bucketName = 'tart-90ca2.appspot.com';
		const userId = req.body.uid;
		const destEmails = req.body.destEmails;
		const srcFilename = `user/${userId}/worksheets/${req.body.filename}`;

		const Firestore = require('@google-cloud/firestore');

		const firestore = new Firestore({
			projectId: projectId,
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const {Storage} = require('@google-cloud/storage');

		const storage = new Storage({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		destEmails.forEach((email) => {
			// Duplicate Emails???
			const destUserId = queryAndFilterEmail(firestore, email)
			let destFilename = `user/${destUserId}/worksheets/${req.body.filename}`;
			const prefix = `user/${destUserId}/worksheets`;

			destFilename = addSuffixIfNameClash(storage, prefix, bucketName, destFilename)

			copyFile(
				storage,
				bucketName,
				srcFilename,
				bucketName,
				destFilename,
			)
		});
	});
}

exports.emptyTrash = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;
		
		const bucketName = 'tart-90ca2.appspot.com';
		const prefix = `user`;

		const {Storage} = require('@google-cloud/storage');

		const storage = new Storage({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const files = await listFilenamesByPrefix(storage, prefix, bucketName);

		const trash = files.filter(filename => /\/[a-z0-9\s_@\-^!#$%&+={}\[\]]+\/trash\//i.test(filename));

		for (const filename of trash) {  
			await storage.bucket(bucketName).file(filename).delete();
		}
		console.log(`Emptied trash.`);
	});
}

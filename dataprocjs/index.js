//
//  dataprocjs.js
//  Tart
//
//  Created by Edbert Dudon on 7/8/19.
//  Copyright © 2019 Project Tart. All rights reserved.
//
//  Deploy:
//  gcloud functions deploy canceljob --runtime nodejs10 --trigger-http --allow-unauthenticated --timeout=540s
//

// const cors = require('cors')({origin: 'https://tart-90ca2.firebaseapp.com'});
const cors = require('cors')({origin: 'https://www.tartcl.com'});
// const cors = require('cors')({origin: '*'});

// goog.require('proto.google.protobuf.Duration')
// const Duration = proto.google.protobuf.Duration()
// const Duration = require('google-protobuf/google/protobuf/duration_pb')

const admin = require('firebase-admin');
admin.initializeApp({
	credential: admin.credential.cert('./tart-90ca2-firebase-adminsdk-kf272-41cfb98e9e.json'),
	databaseURL: 'https://tart-90ca2.firebaseio.com'
})

async function authenticate() {
	try {
		const decodedIdToken = await admin.auth().verifyIdToken(idToken);
		console.log('ID Token correctly decoded', decodedIdToken);
		req.user = decodedIdToken;
	} catch (error) {
		console.error('Error while verifying Firebase ID token:', error);
		res.status(403).send('Unauthorized');
		return;
	}
}

async function listJobs(jobClient, projectId, region, clusterName) {

  const request = {
    projectId: projectId,
    region: region,
    clusterName: clusterName,
  };

  const [operation] = await jobClient.listJobs(request)
  return operation
}

async function getCluster(clusterClient, projectId, region, clusterName) {
  const request = {
    projectId: projectId,
    region: region,
    clusterName: clusterName,
  };

  const [operation] = await clusterClient.getCluster(request)
  return operation
}

async function getJob(jobClient, projectId, region, jobId) {

  const request = {
    projectId: projectId,
    region: region,
    jobId: jobId,
  };

  const [operation] = await jobClient.getJob(request)
  return operation
}

async function cancelJob(jobClient, projectId, region, jobId) {

  const request = {
    projectId: projectId,
    region: region,
    jobId: jobId,
  };

  const operation = await jobClient.cancelJob(request)
  return operation
}

async function decryptAsymmetric(client, versionName, ciphertext) {
  const [result] = await client.asymmetricDecrypt({
	name: versionName,
	ciphertext: ciphertext,
  });

  const plaintext = result.plaintext.toString('utf8');

  console.log(`Plaintext: ${plaintext}`);
  return plaintext;
}

async function getCredentials(firestore, uid) {
	const document = firestore.collection('connections').doc(uid);
	const doc = await document.get()

	return doc.data();
}

// exports.createcluster = async (req, res) => {
//   cors(req, res, async () => {
//     const dataproc = require('@google-cloud/dataproc');
//     const projectId = 'tart-90ca2'
//     const region = 'us-central1'
//     const clusterName = 'cluster-' + req.body.authuser
//     // const duration_message = new duration.Duration()
//     const duration_message = Duration.setSeconds(10800)
//     const status = {status: 'job submitted'}

//     // Create a cluster client with the endpoint set to the desired cluster region
//     const clusterClient = new dataproc.v1beta2.ClusterControllerClient({
//       apiEndpoint: `${region}-dataproc.googleapis.com`,
//       keyFilename: './tart-90ca2-9d37c42ef480.json',
//     });

//     async function createcluster() {

//       const cluster = {
//         projectId: projectId,
//         region: region,
//         cluster: {
//           clusterName: clusterName,
//           config: {
//             masterConfig: {
//               numInstances: 1,
//               machineTypeUri: 'n1-highmem-2',
//               diskConfig: {
//                 bootDiskSizeGb: 20
//               },
//             },
//             workerConfig: {
//               numInstances: 2,
//               machineTypeUri: 'n1-highmem-2',
//               diskConfig: {
//                 bootDiskSizeGb: 20
//               },
//             },
//             lifecycleConfig: {
//               idleDeleteTtl: duration_message
//             },
//             initializationActions: [{
//               executableFile: 'gs://tart-90ca2.appspot.com/scripts/initializationScript.sh'
//             }]
//           },
//         },
//       };

//       const [operation] = await clusterClient.createCluster(cluster)
//       const [response] = await operation.promise()

//       console.log(`Cluster created successfully: ${response.clusterName}`);
//     }

//     await createcluster().catch(err => {
//       console.log(err)
//       status = {status: 'cluster is active'}
//     })

//     // job arguments
//     const worksheet = req.body.worksheet
//     const jobFileArgument = req.body.jobFileArgument
//     const jobFilePath = req.body.jobFilePath
//     const jobFileSave = req.body.jobFileSave

//     // Create a job client with the endpoint set to the desired cluster region
//     const jobClient = new dataproc.v1beta2.JobControllerClient({
//       apiEndpoint: `${region}-dataproc.googleapis.com`,
//       keyFilename: './tart-90ca2-9d37c42ef480.json',
//     });

//     async function submitjob() {

//       const job = {
//         projectId: projectId,
//         region: region,
//         job: {
//           placement: {
//             clusterName: clusterName,
//           },
//           sparkRJob: {
//             mainRFileUri: jobFilePath,
//             args: [
//               jobFileArgument,
//               jobFileSave
//             ]
//           },
//           labels: {
//             "worksheet": worksheet
//           }
//         },
//       };

//       let [jobResp] = await jobClient.submitJob(job);
//       const jobId = jobResp.reference.jobId;

//       console.log(`Submitted job "${jobId}".`);
//     }

//     submitjob().catch(err => {
//       console.log(err)
//       status = {status: 'failed job'}
//     })

//     res.status(200).send(status)
//   })
// };

exports.canceljob = async (req, res) => {
  cors(req, res, async () => {
    const dataproc = require('@google-cloud/dataproc');
    const sleep = require('sleep')
    const projectId = 'tart-90ca2'
    const region = 'us-central1'
    const clusterName = 'cluster-' + req.body.authuser
    let jobId = req.body.jobId;
    const filename = req.body.filename

    // Create a job client with the endpoint set to the desired cluster region
    const jobClient = new dataproc.v1.JobControllerClient({
      apiEndpoint: `${region}-dataproc.googleapis.com`,
      keyFilename: './tart-90ca2-9d37c42ef480.json',
    });

      // if job needs to be cancelled before listJobs replaces 'filler job'
    if (jobId.startsWith('filler job')) {

      const clusterClient = new dataproc.v1beta2.ClusterControllerClient({
        apiEndpoint: `${region}-dataproc.googleapis.com`,
        keyFilename: './tart-90ca2-9d37c42ef480.json',
      });

      let clusterResp = await getCluster(clusterClient, projectId, region, clusterName)
        .catch((err) => {
          console.log('Cluster not found.')

          return('failed retrieve cluster')
        })

      if (clusterResp !== 'failed retrieve cluster') {

        console.log('Waiting for cluster creation...')

        const terminalStates = new Set(['RUNNING', 'STOPPED', 'ERROR', 'DELETING', 'STOPPING'])

        while (!terminalStates.has(clusterResp.status.state)) {
          await sleep.sleep(15);
          clusterResp = await getCluster(clusterClient, projectId, region, clusterName)
            .catch((err) => {
              return('failed retrieve cluster')
            })
        }

        console.log('Cluster created.')
      }

      // Wait for submitJob to execute before cancelling
      const queueStates = new Set(['PENDING', 'SETUP_DONE', 'RUNNING'])

      let jobList
      let filenameList = []
      let isJobInQueue = false

      while(!filenameList.includes(filename) || !isJobInQueue) {
        await sleep.sleep(1);
        jobList = await listJobs(jobClient, projectId, region, clusterName)
          .catch((err) => {
            return([{status: {state: 'failed list jobs'}}])
          })

        // Error because not failed list jobs but just empty []
        if (jobList.length != 0) {
          if (jobList[0].status.state !== 'failed list jobs') {
            filenameList = []

            jobList.forEach(job => {
              filenameList.push(job.labels.worksheet)

              if (queueStates.has(job.status.state)) {
                isJobInQueue = true

                if (job.labels.worksheet === filename) {
                  jobId = job.reference.jobId
                }
              }
            });
          }
        }
      }

      console.log(`Replaced filler job ID with "${jobId}".`);
    }

    let jobResp = await cancelJob(jobClient, projectId, region, jobId)
    .catch((err) => {
      return([{status: {state: 'job not found'}}])
    })

    // For cases where user instantly cancels job before createandsubmit gets to finish running
    if (jobResp[0].status.state === 'job not found') {

      jobResp = await getJob(jobClient, projectId, region, jobId)
        .catch((err) => {
          return([{status: {state: 'job not found'}}])
        })

      if (jobResp[0].status.state === 'job not found') {

        const terminalStates = new Set(['RUNNING', 'DONE', 'ERROR', 'CANCELLED'])

        while (!terminalStates.has(jobResp[0].status.state)) {
          await sleep.sleep(1);
          jobResp = await getJob(jobClient, projectId, region, jobId)
            .catch((err) => {
              return([{status: {state: 'job not found'}}])
            })
        }
      }

      await cancelJob(jobClient, projectId, region, jobId)
        .catch((err) => {
          console.log('Job not found')
        })
    }

    console.log(`Cancelled job "${jobId}".`);
  })
};

exports.listjobs = async (req,res) => {
  cors(req, res, async () => {
    const dataproc = require('@google-cloud/dataproc');
    const sleep = require('sleep')
    const projectId = 'tart-90ca2'
    const region = 'us-central1'
    const clusterName = 'cluster-' + req.body.authuser

    // Create a job client with the endpoint set to the desired cluster region
    const jobClient = new dataproc.v1beta2.JobControllerClient({
      apiEndpoint: `${region}-dataproc.googleapis.com`,
      keyFilename: './tart-90ca2-9d37c42ef480.json',
    });

    let jobResp = await listJobs(jobClient, projectId, region, clusterName)
      .catch((err) => {
        return([{status:'failed list jobs'}])
      })

    if (jobResp[0].status === 'failed list jobs') {
      const clusterClient = new dataproc.v1beta2.ClusterControllerClient({
        apiEndpoint: `${region}-dataproc.googleapis.com`,
        keyFilename: './tart-90ca2-9d37c42ef480.json',
      });

      let clusterResp = await getCluster(clusterClient, projectId, region, clusterName)
        .catch((err) => {
          console.log('Cluster not found.')

          return('failed retrieve cluster')
        })

      if (clusterResp !== 'failed retrieve cluster') {

        console.log('Waiting for cluster creation...')

        const terminalStates = new Set(['RUNNING', 'STOPPED', 'ERROR', 'DELETING', 'STOPPING'])

        while (!terminalStates.has(clusterResp.status.state)) {
          await sleep.sleep(15);
          clusterResp = await getCluster(clusterClient, projectId, region, clusterName)
            .catch((err) => {
              return('failed retrieve cluster')
            })
        }

        console.log('Cluster created.')

        jobResp = await listJobs(jobClient, projectId, region, clusterName)
          .catch((err) => {
            return([{status:'failed list jobs'}])
          })
      }
    }

    res.setHeader('Content-Type', 'application/json');
    res.status(200).send(jobResp);
  })
}

exports.copysample = async (req,res) => {
  cors(req, res, async () => {
    const {Storage} = require('@google-cloud/storage');

    const storage = new Storage();

    const userId = req.body.uid
    const destFilename = 'user/' + userId + '/pit pd.csv'

    await storage
      .bucket('tart-90ca2.appspot.com')
      .file('scripts/pit pd.csv')
      .copy(storage.bucket('tart-90ca2.appspot.com').file(destFilename))
  })
}

exports.decryptAsymmetricR = async (req,res) => {
	cors(req, res, async () => {
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

		// credentials = credentials[localhost:3306]

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
}

exports.testauth = async (req,res) => {
	cors(req, res, async () => {

		const admin = require('firebase-admin');
		admin.initializeApp({
		  credential: admin.credential.applicationDefault(),
		  databaseURL: 'https://tart-90ca2.firebaseio.com'
		})

		console.log('Check if request is authorized with Firebase ID token');

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
			console.log('Found "Authorization" header');
			// Read the ID Token from the Authorization header.
			idToken = req.headers.authorization.split('Bearer ')[1];
		} else if(req.cookies) {
			console.log('Found "__session" cookie');
			// Read the ID Token from cookie.
			idToken = req.cookies.__session;
		} else {
			// No cookie
			res.status(403).send('Unauthorized');
			return;
		}

		try {
			const decodedIdToken = await admin.auth().verifyIdToken(idToken);
			console.log('ID Token correctly decoded', decodedIdToken);
			req.user = decodedIdToken;
			next();
			return;
		} catch (error) {
			console.error('Error while verifying Firebase ID token:', error);
			res.status(403).send('Unauthorized');
			return;
		}
	})
}

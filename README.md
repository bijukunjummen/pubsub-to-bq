## Create a Dataflow job
```shell
RUNNER=DataflowRunner
PROJECT_ID="fill project id here"
BUCKET_NAME="fill in a valid bucket name here"
PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-bigquery
USE_SUBSCRIPTION=true

./mvnw compile exec:java \
-Dexec.mainClass=org.bk.dataflow.bq.templates.PubSubToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT_ID} \
--region=us-west1 \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER}/temp \
--templateLocation=${PIPELINE_FOLDER}/template \
--runner=${RUNNER} \
--useSubscription=${USE_SUBSCRIPTION} \
```

## Execute the dataflow job
```shell
JOB_NAME=pubsub-to-bigquery-`date +"%Y%m%d-%H%M%S%z"`
gcloud dataflow jobs run ${JOB_NAME} \
--gcs-location=${PIPELINE_FOLDER}/template \
--worker-region=us-west1 \
--region=us-west1 \
--parameters "inputSubscription=projects/${PROJECT_ID}/subscriptions/sample-dataflow-subscription,outputTableSpec=${PROJECT_ID}:dataflowsample.pubsubsourced"
```
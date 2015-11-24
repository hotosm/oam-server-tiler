#!/bin/sh

# Process a set of imagery using a one-off cluster

MASTER_INSTANCE_TYPE=m3.xlarge
WORKER_INSTANCE_TYPE=m3.xlarge
WORKER_COUNT=1
# WORKER_COUNT=2
SPOT_WORKER_PRICE=0.15
SPOT_WORKER_COUNT=1
# SPOT_WORKER_COUNT=8

DRIVER_MEMORY=2g
NUM_EXECUTORS=40
EXECUTOR_MEMORY=2304m
EXECUTOR_CORES=1

JOB_ID=20151012-1

REQUEST_URI=s3://workspace-oam-hotosm-org/test-req-partial-${JOB_ID}.json
WORKSPACE_URI=s3://workspace-oam-hotosm-org/emr-test-job-partial-${JOB_ID}

# write job configuration to S3 as REQUEST_URI
cat <<EOF | aws s3 cp - $REQUEST_URI
{
    "jobId": "${JOB_ID}",
    "target": "s3://oam-tiles/${JOB_ID}",
    "images": [
        "http://hotosm-oam.s3.amazonaws.com/356f564e3a0dc9d15553c17cf4583f21-12.tif",
        "http://oin-astrodigital.s3.amazonaws.com/LC81420412015111LGN00_bands_432.TIF"
    ],
    "workspace": "${WORKSPACE_URI}"
}
EOF

CLUSTER_ID=$(aws emr create-cluster \
  --name "OAM Tiler - ${JOB_ID}" \
  --log-uri s3://oam-server-tiler/emr/logs/ \
  --release-label emr-4.2.0 \
  --use-default-roles \
  --auto-terminate \
  --ec2-attributes KeyName=oam-mojodna \
  --applications Name=Spark \
  --instance-groups \
    Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE_TYPE \
    Name=ReservedWorkers,InstanceCount=$WORKER_COUNT,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE_TYPE \
    Name=SpotWorkers,InstanceCount=$SPOT_WORKER_COUNT,BidPrice=$SPOT_WORKER_PRICE,InstanceGroupType=TASK,InstanceType=$WORKER_INSTANCE_TYPE \
  --bootstrap-action Path=s3://oam-server-tiler/emr/bootstrap-v0.2.0.sh \
  --configurations http://s3.amazonaws.com/oam-server-tiler/emr/configurations.json \
  --steps \
    Name=CHUNK,ActionOnFailure=TERMINATE_CLUSTER,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,s3://oam-server-tiler/emr/chunk.py,$REQUEST_URI] \
    Name=MOSAIC,ActionOnFailure=TERMINATE_CLUSTER,Type=Spark,Jar=s3://oam-server-tiler/emr/mosaic.jar,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,--class,org.hotosm.oam.Main,s3://oam-server-tiler/emr/mosaic.jar,$WORKSPACE_URI/step1_result.json] | jq -r .ClusterId)

echo "Cluster ID: ${CLUSTER_ID}"

# TODO poll cluster status
aws emr describe-cluster --cluster-id $CLUSTER_ID | jq -r '.Cluster.Status.State + ": " + .Cluster.Status.StateChangeReason.Message'

# TODO fetch $WORKSPACE_URI/step1_result.json

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

JOB_ID=rob-test-jan-2016

REQUEST_URI=s3://oam-server-tiler/test-files/test-req-partial-${JOB_ID}.json
WORKSPACE_URI=s3://oam-server-tiler/workspace/emr-test-job-partial-${JOB_ID}

# write job configuration to S3 as REQUEST_URI
cat <<EOF | aws s3 cp - $REQUEST_URI
{
    "jobId": "${JOB_ID}",
    "target": "s3://oam-tiles/${JOB_ID}",
    "images": [
        "s3://hotosm-oam/uploads/2016-01-05/568b113a63ebf4bc00074e67/scene/0/scene-0-image-0-Nick-Do-transparent_block_alpha_mosaic_group1.tif"
    ],
    "workspace": "${WORKSPACE_URI}"
}
EOF

CLUSTER_ID=$(aws emr create-cluster \
  --name "OAM Tiler - ${JOB_ID}" \
  --log-uri s3://oam-server-tiler/emr/logs/ \
  --release-label emr-4.1.0 \
  --use-default-roles \
  --auto-terminate \
  --ec2-attributes KeyName=oam-mojodna \
  --applications Name=Spark \
  --instance-groups \
    Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE_TYPE \
    Name=ReservedWorkers,InstanceCount=$WORKER_COUNT,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE_TYPE \
    Name=SpotWorkers,InstanceCount=$SPOT_WORKER_COUNT,BidPrice=$SPOT_WORKER_PRICE,InstanceGroupType=TASK,InstanceType=$WORKER_INSTANCE_TYPE \
  --bootstrap-action Path=s3://oam-server-tiler/emr/bootstrap.sh \
  --configurations http://s3.amazonaws.com/oam-server-tiler/emr/configurations.json \
  --steps \
    Name=CHUNK,ActionOnFailure=TERMINATE_CLUSTER,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,s3://oam-server-tiler/emr/chunk.py,$REQUEST_URI] \
    Name=MOSAIC,ActionOnFailure=TERMINATE_CLUSTER,Type=Spark,Jar=s3://oam-server-tiler/emr/mosaic.jar,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,--class,org.hotosm.oam.Main,s3://oam-server-tiler/emr/mosaic.jar,$WORKSPACE_URI/step1_result.json] | jq -r .ClusterId)

echo "Cluster ID: ${CLUSTER_ID}"

# TODO poll cluster status
aws emr describe-cluster --cluster-id $CLUSTER_ID | jq -r '.Cluster.Status.State + ": " + .Cluster.Status.StateChangeReason.Message'

# TODO fetch $WORKSPACE_URI/step1_result.json

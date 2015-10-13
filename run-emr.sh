MASTER_INSTANCE=m3.xlarge
MASTER_PRICE=0.15

WORKER_INSTANCE=m3.xlarge
WORKER_PRICE=0.15
WORKER_COUNT=10

DRIVER_MEMORY=2g
NUM_EXECUTORS=40
EXECUTOR_MEMORY=2304m
EXECUTOR_CORES=1

REQUEST_URI=s3://oam-server-tiler/testfiles/test-req-partial.json
WORKSPACE_URI=s3://oam-server-tiler/workspace/emr-test-job-partial

aws emr create-cluster \
  --name "OAM Tiler" \
  --log-uri s3://workspace-oam-hotosm-org/logs/ \
  --release-label emr-4.0.0 \
  --use-default-roles \
  --ec2-attributes KeyName=oam-emanuele \
  --applications Name=Spark \
  --instance-groups \
    Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE \
    Name=Workers,InstanceCount=$WORKER_COUNT,BidPrice=$WORKER_PRICE,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE \
  --bootstrap-action Path=s3://oam-server-tiler/emr/bootstrap.sh \
  --configurations file://./emr.json
  --steps \
    Name=CHUNK,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,s3://oam-server-tiler/emr/chunk.py,$REQUEST_URI] \
    Name=MOSAIC,ActionOnFailure=CONTINUE,Type=Spark,Jar=s3://oam-server-tiler/emr/mosaic.jar,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,--class,org.hotosm.oam.Main,s3://oam-server-tiler/emr/mosaic.jar,$WORKSPACE_URI/step1_result.json]

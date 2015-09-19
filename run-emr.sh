MASTER_INSTANCE=m3.xlarge
MASTER_PRICE=0.15

WORKER_INSTANCE=m3.xlarge
WORKER_PRICE=0.15

# This is how many works beyond the 2 reserve instances
WORKER_COUNT=8

DRIVER_MEMORY=4G
NUM_EXECUTORS=20
EXECUTOR_MEMORY=5G
EXECUTOR_CORES=2

aws emr create-cluster \
  --name "OAM Tiler" \
  --log-uri s3://workspace-oam-hotosm-org/logs/ \
  --release-label emr-4.0.0 \
  --use-default-roles \
  --auto-terminate \
  --ec2-attributes KeyName=oam-emanuele \
  --applications Name=Spark \
  --instance-groups \
    Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE \
    Name=ReservedWorkers,InstanceCount=2,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE \
    Name=SpotWorkers,InstanceCount=$WORKER_COUNT,BidPrice=$WORKER_PRICE,InstanceGroupType=TASK,InstanceType=$WORKER_INSTANCE \
  --bootstrap-action Path=s3://oam-tiler-emr/bootstrap.sh \
  --configurations file://./emr.json \
  --steps \
  Name=CHUNK,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,s3://oam-tiler-emr/chunk.py,s3://workspace-oam-hotosm-org/test-req-full.json] \
  Name=MOSAIC,ActionOnFailure=CONTINUE,Type=Spark,Jar=s3://oam-tiler-emr/mosaic.jar,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,--class,org.hotosm.oam.Main,s3://oam-tiler-emr/mosaic.jar,s3://workspace-oam-hotosm-org/emr-test-job-full/step1_result.json]

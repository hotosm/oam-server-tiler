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
    InstanceCount=1,BidPrice=0.150,InstanceGroupType=MASTER,InstanceType=m3.xlarge \
    InstanceCount=10,BidPrice=0.150,InstanceGroupType=CORE,InstanceType=m3.xlarge \
  --bootstrap-action Path=s3://oam-tiler-emr/bootstrap.sh \
  --configurations file://./emr.json \
  --steps \
  Name=CHUNK,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,s3://oam-tiler-emr/chunk.py,s3://workspace-oam-hotosm-org/test-req-full.json] \
  Name=MOSAIC,ActionOnFailure=CONTINUE,Type=Spark,Jar=s3://oam-tiler-emr/mosaic.jar,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,--class,org.hotosm.oam.Main,s3://oam-tiler-emr/mosaic.jar,s3://workspace-oam-hotosm-org/emr-test-job-full/step1_result.json]

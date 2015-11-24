# Starts a long running ETL cluster.

MASTER_INSTANCE=m3.xlarge
MASTER_PRICE=0.15

WORKER_INSTANCE=m3.xlarge
WORKER_COUNT=2
SPOT_WORKER_PRICE=0.15
SPOT_WORKER_COUNT=8

aws emr create-cluster \
  --name "OAM Tiler" \
  --log-uri s3://oam-server-tiler/emr/logs/ \
  --release-label emr-4.2.0 \
  --use-default-roles \
  --ec2-attributes KeyName=oam-mojodna \
  --applications Name=Spark \
  --instance-groups \
    Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE \
    Name=ReservedWorkers,InstanceCount=$WORKER_COUNT,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE \
    Name=SpotWorkers,InstanceCount=$SPOT_WORKER_COUNT,BidPrice=$SPOT_WORKER_PRICE,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE \
  --bootstrap-action Path=s3://oam-server-tiler/emr/bootstrap-v0.2.0.sh \
  --configurations s3://oam-server-tiler/emr/configurations.js

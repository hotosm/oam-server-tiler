# Starts a long running ETL cluster.

MASTER_INSTANCE=m3.xlarge
MASTER_PRICE=0.15

WORKER_INSTANCE=m3.xlarge
WORKER_PRICE=0.15
WORKER_COUNT=10

aws emr create-cluster \
  --name "OAM Tiler" \
  --log-uri s3://oam-server-tiler/emr/logs/ \
  --release-label emr-4.0.0 \
  --use-default-roles \
  --ec2-attributes KeyName=oam-emanuele \
  --applications Name=Spark \
  --instance-groups \
    Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE \
    Name=Workers,InstanceCount=$WORKER_COUNT,BidPrice=$WORKER_PRICE,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE \
  --bootstrap-action Path=s3://oam-server-tiler/emr/bootstrap.sh \
  --configurations s3://oam-server-tiler/emr/configurations.js

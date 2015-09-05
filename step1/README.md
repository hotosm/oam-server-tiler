To run this on EMR, do something akin to the following (with buckets changed as appropriate):

```bash
aws emr create-cluster \
  --name "NED Conversion" \
  --log-uri s3://ned-13arcsec.openterrain.org/logs/ \
  --release-label emr-4.0.0 \
  --use-default-roles \
  --auto-terminate \
  --ec2-attributes KeyName=stamen-keypair \
  --applications Name=Spark \
  --instance-groups \
    InstanceCount=1,InstanceGroupType=MASTER,InstanceType=m3.xlarge \
    InstanceCount=4,BidPrice=0.200,InstanceGroupType=CORE,InstanceType=c3.2xlarge \
  --bootstrap-action Path=s3://ned-13arcsec.openterrain.org/spark/bootstrap.sh \
  --steps Name=NED,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,s3://ned-13arcsec.openterrain.org/spark/chunk.py] \
  --configurations file://./emr.json
```
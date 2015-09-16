CLUSTER_ID=j-RRHMKKWUYLTT

DRIVER_MEMORY=2g
NUM_EXECUTORS=40
EXECUTOR_MEMORY=2304m
EXECUTOR_CORES=1

aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps \
    Name=CHUNK,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,s3://oam-tiler-emr/chunk.py,s3://workspace-oam-hotosm-org/test-req3.json] \
    Name=MOSAIC,ActionOnFailure=CONTINUE,Type=Spark,Jar=s3://oam-tiler-emr/mosaic.jar,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,--class,org.hotosm.oam.Main,s3://oam-tiler-emr/mosaic.jar,s3://workspace-oam-hotosm-org/emr-test-job-full/step1_result.json]

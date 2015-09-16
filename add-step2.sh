CLUSTER_ID=j-RRHMKKWUYLTT

DRIVER_MEMORY=3G
NUM_EXECUTORS=40
EXECUTOR_MEMORY=3G
EXECUTOR_CORES=1

aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps \
    Name=MOSAIC,ActionOnFailure=CONTINUE,Type=Spark,Jar=s3://oam-tiler-emr/mosaic.jar,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,--class,org.hotosm.oam.Main,s3://oam-tiler-emr/mosaic.jar,s3://workspace-oam-hotosm-org/emr-test-job-full/step1_result.json]

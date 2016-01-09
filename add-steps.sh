CLUSTER_ID=j-38B7UZSX1K8Z3

DRIVER_MEMORY=2g
NUM_EXECUTORS=40
EXECUTOR_MEMORY=2304m
EXECUTOR_CORES=1

REQUEST_URI=s3://oam-server-tiler/testfiles/test-req-partial.json
WORKSPACE_URI=s3://oam-server-tiler/workspace/emr-test-job-partial

aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps \
    Name=CHUNK,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,s3://oam-server-tiler/emr/chunk.py,$REQUEST_URI] \
    Name=MOSAIC,ActionOnFailure=CONTINUE,Type=Spark,Jar=s3://oam-server-tiler/emr/mosaic.jar,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,--class,org.hotosm.oam.Main,s3://oam-server-tiler/emr/mosaic.jar,$WORKSPACE_URI/step1_result.json]

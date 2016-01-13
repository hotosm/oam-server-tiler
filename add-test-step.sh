CLUSTER_ID=j-34T4JOTY45LCR

DRIVER_MEMORY=2g
NUM_EXECUTORS=40
EXECUTOR_MEMORY=2304m
EXECUTOR_CORES=1

REQUEST_URI=s3://oam-server-tiler/requests/8e662d19-07cd-4a0d-a7a1-cec872f13f28.json
WORKSPACE_URI=s3://oam-server-tiler/workspace/8e662d19-07cd-4a0d-a7a1-cec872f13f28
# REQUEST_URI=s3://oam-server-tiler/requests/b79c9412-d60e-40bd-a88f-b3bfd1840ae3.json
# WORKSPACE_URI=s3://oam-server-tiler/workspace/b79c9412-d60e-40bd-a88f-b3bfd1840ae3

aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps \
    Name=CHUNK,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,s3://oam-server-tiler/test.py]

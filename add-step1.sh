CLUSTER_ID=j-15976XB73IB3I

DRIVER_MEMORY=2g
NUM_EXECUTORS=40
EXECUTOR_MEMORY=2304m
EXECUTOR_CORES=1

REQUEST_URI=s3://oam-server-tiler/requests/ac548563-e1c4-4e11-8e8a-661167beed18.json
WORKSPACE_URI=s3://oam-server-tiler/workspace/ac548563-e1c4-4e11-8e8a-661167beed18
# REQUEST_URI=s3://oam-server-tiler/requests/b79c9412-d60e-40bd-a88f-b3bfd1840ae3.json
# WORKSPACE_URI=s3://oam-server-tiler/workspace/b79c9412-d60e-40bd-a88f-b3bfd1840ae3

aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps \
    Name=CHUNK,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,s3://oam-server-tiler/emr/chunk.py,$REQUEST_URI]

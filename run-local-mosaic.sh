# 13 min
time spark-submit \
    --master local[*] \
    --executor-memory 5G \
    --driver-memory 4g \
    ./mosaic/target/scala-2.10/oam-tiler-assembly-0.1.0.jar \
    /Users/rob/proj/oam/data/workspace/step1_result.json
#    s3://workspace-oam-hotosm-org/step1_result.json

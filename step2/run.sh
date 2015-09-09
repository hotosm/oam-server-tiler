# 13 min
time spark-submit \
    --master local[*] \
    --executor-memory 5G \
    --driver-memory 4g \
    ./target/scala-2.10/oam-tiler-assembly-0.1.0.jar \
    /Users/rob/proj/oam/data/workspace/step1_result.json

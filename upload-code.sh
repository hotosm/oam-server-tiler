TARGET=s3://oam-server-tiler/emr
VERSION=0.1.1

cd mosaic && ./sbt assembly
aws s3 cp chunk/chunk.py $TARGET/chunk-${VERSION}.py
aws s3 cp mosaic/target/scala-2.10/oam-tiler-assembly-$VERSION.jar $TARGET/mosaic-${VERSION}.jar
aws s3 cp emr.json $TARGET/configurations-${VERSION}.json

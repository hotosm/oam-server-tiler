TARGET=s3://oam-server-tiler/emr
VERSION=0.1.0

cd mosaic
./sbt assembly
cd ..
aws s3 cp chunk/chunk.py $TARGET/chunk.py
aws s3 cp mosaic/target/scala-2.10/oam-tiler-assembly-$VERSION.jar $TARGET/mosaic.jar

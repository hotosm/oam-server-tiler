#!/bin/sh -e

sudo yum-config-manager --enable epel
sudo yum -y install geos proj proj-nad proj-epsg
sudo ln -s /usr/lib64/libproj.so.0 /usr/lib64/libproj.so
aws s3 cp s3://oam-server-tiler/emr/gdal-1.11.2-amz1.tar.gz - | sudo tar zxf - -C /usr/local
sudo GDAL_CONFIG=/usr/local/bin/gdal-config pip-2.7 install boto3 rasterio mercantile psutil

sudo yum -y install inotify-tools

cat <<EOF > /tmp/update-pyspark
#!/bin/sh

# create the target directory so we can watch it
sudo mkdir -p /usr/lib/spark/python/lib

# download the replacement zip
aws s3 cp s3://oam-server-tiler/emr/pyspark-1.5.1.zip /tmp/pyspark-1.5.1.zip

# wait for yum to install spark-core
inotifywait -e create /usr/lib/spark/python/lib

# allow some time for writing to complete
sleep 5

# replace pyspark.zip
sudo mv /tmp/pyspark-1.5.1.zip /usr/lib/spark/python/lib/pyspark.zip
EOF

chmod +x /tmp/update-pyspark

# background pyspark updating so that bootstrapping can complete
nohup /tmp/update-pyspark &

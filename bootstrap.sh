#!/bin/sh -e

sudo yum-config-manager --enable epel
sudo yum -y install geos proj proj-nad proj-epsg libcurl-devel.x86_64
sudo ln -s /usr/lib64/libproj.so.0 /usr/lib64/libproj.so
aws s3 cp s3://oam-server-tiler/emr/gdal-1.11.2-amz2.tar.gz - | sudo tar zxf - -C /usr/local
aws s3 cp s3://oam-server-tiler/emr/gdal-1.11.2-amz2-py.tar.gz - | sudo tar zxf - -C /usr/local/lib64/python2.6/site-packages
sudo GDAL_CONFIG=/usr/local/bin/gdal-config pip-2.7 install boto3 rasterio==0.29.0 mercantile psutil

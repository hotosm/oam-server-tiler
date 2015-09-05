import os, sys
import errno
import json
import itertools
import math
import multiprocessing
from urlparse import urlparse
from collections import namedtuple

import boto3
import mercantile
import numpy
import rasterio
from rasterio import crs
from rasterio.transform import from_bounds
from rasterio.warp import (reproject, RESAMPLING, calculate_default_transform, transform)
from rasterio._io import virtual_file_to_buffer

from pyspark.accumulators import AccumulatorParam

APP_NAME = "Reproject and chunk"
CHUNK_SIZE = 1024
OUTPUT_FILE_NAME = "step1_result.json"

def get_filename(uri):
    return os.path.splitext(os.path.basename(uri))[0]

class ImageSourceAccumulatorParam(AccumulatorParam):
    """
    Accumulator that will collect our image data that will be
    included as part of the input to the next stage of processing.
    """
    def zero(self, image_source):
        return [image_source]

    def addInPlace(self, sources1, sources2):
        res = []
        res.extend(sources1)
        res.extend(sources2)
        return res

def mkdir_p(dir):
    try:
        os.makedirs(dir)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(dir):
            pass
        else: raise


def process_chunk(tile, input, creation_options, resampling="near", out_dir="."):
    """Process a single tile."""

    input = input.replace("s3://", "/vsicurl/http://s3.amazonaws.com/")
    input_uri = urlparse(input)

    if input_uri.scheme == "s3":
        client = boto.client("s3")

        bucket = input_uri.netloc
        key = input_uri.path[1:]

        response = client.head_object(
            Bucket=bucket,
            Prefix=key
        )

        if response.get("Contents") is not None:
            return

    print tile

    # Get the bounds of the tile.
    ulx, uly = mercantile.xy(
        *mercantile.ul(tile.x, tile.y, tile.z))
    lrx, lry = mercantile.xy(
        *mercantile.ul(tile.x + 1, tile.y + 1, tile.z))

    tmp_path = "/vsimem/tile"

    with rasterio.open(input, "r") as src:
        meta = src.meta.copy()
        meta.update(creation_options)
        meta["height"] = CHUNK_SIZE
        meta["width"] = CHUNK_SIZE
        meta["transform"] = from_bounds(ulx, lry, lrx, uly, CHUNK_SIZE, CHUNK_SIZE)

        with rasterio.open(tmp_path, "w", **meta) as tmp:
            # Reproject the src dataset into image tile.
            for bidx in src.indexes:
                reproject(
                    source=src.read_band(bidx, window),
                    destination=rasterio.band(tmp, bidx),
                    resampling=RESAMPLING.bilinear,
                    num_threads=multiprocessing.cpu_count() / 2,
                )

            # check for chunks contain only NODATA
            tile_data = tmp.read()
            if tile_data.all() and tile_data[0][0][0] == src.nodata:
                return

    output_uri = urlparse(out_dir)
    contents = bytearray(virtual_file_to_buffer(tmp_path))

    if output_uri.scheme == "s3":
        client = boto3.client("s3")

        bucket = output_uri.netloc
        key = "%s/%d/%d/%d.tif" % (output_uri.path[1:], tile.z, tile.x, tile.y)

        response = client.put_object(
            ACL="public-read",
            Body=bytes(contents),
            Bucket=bucket,
            # CacheControl="TODO",
            ContentType="image/tiff",
            Key=key
        )
    else:
        output_path = os.path.join(out_dir, "%d/%d/%d.tif" % (tile.z, tile.x, tile.y))
        mkdir_p(os.path.dirname(output_path))

        f = open(output_path, "w")
        f.write(contents)
        f.close()


def get_zoom(input, dst_crs="EPSG:3857"):
    input = input.replace("s3://", "/vsicurl/http://s3.amazonaws.com/")
    with rasterio.drivers():
        with rasterio.open(input) as src:
            # Compute the geographic bounding box of the dataset.
            (west, east), (south, north) = transform(
                src.crs, "EPSG:4326", src.bounds[::2], src.bounds[1::2])

            affine, _, _ = calculate_default_transform(src.crs, dst_crs,
                src.width, src.height, *src.bounds, resolution=None)

            # grab the lowest resolution dimension
            resolution = max(abs(affine[0]), abs(affine[4]))

            return int(round(math.log((2 * math.pi * 6378137) /
                                      (resolution * CHUNK_SIZE)) / math.log(2)))


def get_tiles(zoom, input, dst_crs="EPSG:3857"):
    print "getting tiles for", input
    input = input.replace("s3://", "/vsicurl/http://s3.amazonaws.com/")
    with rasterio.drivers():
        with rasterio.open(input) as src:
            # Compute the geographic bounding box of the dataset.
            (west, east), (south, north) = transform(
                src.crs, "EPSG:4326", src.bounds[::2], src.bounds[1::2])

            # Initialize an iterator over output tiles.
            return mercantile.tiles(
                west, south, east, north, range(zoom, zoom + 1))


def chunk(input, out_dir):
    """
    Intended for conversion from whatever the source format is to matching
    filenames containing 4326 data, etc.
    """
    resampling = "bilinear"

    creation_options = {
        "driver": "GTiff",
        "crs": "EPSG:3857",
        "tiled": True,
        "compress": "deflate",
        "predictor":   2, # 3 for floats, 2 otherwise
        "sparse_ok": True,
        "blockxsize": 256,
        "blockysize": 256,
    }

    tiles = get_tiles(input, dst_crs=creation_options["crs"])

    outputs = [process_chunk(tile, input, creation_options, resampling=resampling, out_dir=out_dir) for tile in tiles]

    outputs = filter(lambda x: x is not None, outputs)

    print outputs


def main(sc, input, out_dir):
    creation_options = {
        "driver": "GTiff",
        "crs": "EPSG:3857",
        "tiled": True,
        "compress": "deflate",
        "predictor":   2, # 3 for floats, 2 otherwise
        "sparse_ok": True,
        "blockxsize": 256,
        "blockysize": 256,
    }

    zoom = get_zoom(input)

    client = boto3.client("s3")

    paginator = client.get_paginator("list_objects")
    source_pages = paginator.paginate(Bucket="ned-13arcsec.openterrain.org", Prefix="4326/")

    tiles = sc.parallelize(source_pages).flatMap(lambda page: page["Contents"]).map(lambda item: "s3://ned-13arcsec.openterrain.org/" + item["Key"]).repartition(sc.defaultParallelism).flatMap(lambda source: get_tiles(zoom, source)).distinct().cache()

    tiles.foreach(lambda tile: process_chunk(tile, input, creation_options, resampling="bilinear", out_dir=out_dir))

ImageSource = namedtuple('ImageSource', "source_uri shape res bounds imageFolder crs")
ChunkTask = namedtuple('ChunkTask', "source_uri window bounds target")

def generate_chunk_tasks(image_source, tile_dim):
    tasks = []
    (cols, rows) = image_source.shape
    tile_cols = cols / tile_dim
    tile_rows = rows / tile_dim
    for tile_row in range(0, tile_rows):
        for tile_col in range(0, tile_cols):
            
            start_row = tile_row * tile_dim
            start_col = tile_col * tile_dim
            target_name = "%s-%d-%d.tif" % (get_filename(image_source.source_uri), tile_col, tile_row)
            target = os.path.join(image_source.imageFolder, target_name)
            task = (image_source.source_uri, ((start_row, start_row + tile_dim), (start_col, start_col + tile_dim)), target)
            tasks.append(task)

    return tasks
    

def create_image_source(source_uri, imageFolder):
    with rasterio.drivers():
        with rasterio.open(source_uri) as src:
            shape = src.shape
            res = src.res
            bounds = src.bounds
            return ImageSource(source_uri=source_uri,
                               shape=shape, res=res,
                               bounds=bounds,
                               imageFolder=imageFolder,
                               crs=src.crs)

# Construct result
def construct_image_info(image_source):
    image_source
    cellSize = { "width" : image_source.res[0], "height": image_source.res[1] }
    extent = { "xmin": image_source.bounds.left, "ymin": image_source.bounds.top,
               "xmax": image_source.bounds.right, "ymax": image_source.bounds.bottom }
    return {
        "cellSize" : cellSize,
        "extent" : extent,
        "imageFolder": image_source.imageFolder
    }

    

# class ImageSource(object):
#     def __init__(self, source_uri, shape, res, bounds, imageFolder):
#         self.source_uri = source_uri
#         self.shape = shape
#         self.res = res
#         self.bounds = bounds
#         self.imageFolder = imageFolder

#     def generate_chunk_tasks(self, tile_dim):
#         tasks = []
#         (cols, rows) = self.shape
#         tile_cols = cols / tile_dim
#         tile_rows = rows / tile_dim
#         for tile_row in range(0, tile_rows):
#             for tile_col in range(0, tile_cols):
#                 start_row = tile_row * tile_dim
#                 start_col = tile_col * tile_dim
#                 target_name = "%s-%d-%d.tif" % (get_filename(source_uri), tile_col, tile_row)
#                 target = os.path.join(self.imageFolder, target_name)
#                 task = (source_uri, ([start_row, start_row + tile_dim], [start_col, start_col + tile_dim]), target)
#                 tasks.append(task)

#     @classmethod
#     def from_source_uri(cls, source_uri, imageFolder):
#         with rasterio.drivers():
#             with rasterio.open(source) as src:
#                 shape = src.shape
#                 res = src.res
#                 bounds = src.bounds
#                 return cls(source_uri, shape, source, res, bounds, imageFolder)
            

def process_chunk_task(task):
    """
    Chunks the image into tile_dim x tile_dim tiles,
    and saves them to the target folder (s3 or local)
    """

    (source_uri, window, target) = task

    creation_options = {
        "driver": "GTiff",
        "crs": "EPSG:3857",
        "tiled": False,
        "compress": "deflate",
        "predictor":   2, # 3 for floats, 2 otherwise
        "sparse_ok": True
    }

    with rasterio.open(source_uri, "r") as src:
        src_cols = src.meta['width']
        src_rows = src.meta['height']
        src_affine = src.affine
        (xmin, ymax) = src_affine * (0, 0)

        def get_affine(col, row):
            (tx, ty) = affine * (col, row)
            ta = affine.translation(tx - xmin, ty - ymax) * affine
            (ntx, nty) = ta * (0, 0)
            return ta

        meta = src.meta.copy()
        meta.update(creation_options)
        meta["height"] = CHUNK_SIZE
        meta["width"] = CHUNK_SIZE

        tmp_path = "/vsimem/" + get_filename(target)
#        tmp_path = "/vsimem/tile"

        with rasterio.open(tmp_path, "w", **meta) as tmp:
            # Reproject the src dataset into image tile.
            for bidx in src.indexes:
                # source = src.read_band(bidx, window=window)
                source = rasterio.band(tmp, bidx)
                destination = rasterio.band(tmp, bidx)
                # destination = numpy.zeros_like(source)

                reproject(
                    source=source,
                    destination=destination,
                    resampling=RESAMPLING.bilinear
                )

            # check for chunks contain only NODATA
            tile_data = tmp.read()
            if tile_data.all() and tile_data[0][0][0] == src.nodata:
                return

    contents = bytearray(virtual_file_to_buffer(tmp_path))

    parsed_target = urlparse(target)

    if parsed_target.scheme == "s3":
        client = boto3.client("s3")

        bucket = parsed_target.netloc
        key = parsed_target.path[1:]

        response = client.put_object(
            ACL="public-read",
            Body=bytes(contents),
            Bucket=bucket,
            # CacheControl="TODO",
            ContentType="image/tiff",
            Key=key
        )
    else:
        output_path = target
        mkdir_p(os.path.dirname(output_path))

        with open(output_path, "w") as f:
            f.write(contents)

def process_uris(images, workspace_uri):
    """
    Returns a list of tuples which is the GDAL-readable URI for each image,
    as well as a target workspace folder to place the chunked images.
    """
    result = []
    workspace_keys = []
    for uri in images:
        # Get the GDAL readable URI
        parsed = urlparse(uri)
        result_uri = ""
        if not parsed.scheme:
            result_uri = uri
        else:
            if parsed.scheme == "s3":
                raise Error("Not implemented") #TODO
            elif parsed.scheme == "http":
                raise Error("Not implemented") #TODO
            else:
                raise Error("Unsupported scheme: %s" % parsed.schem)

        # Get the workspace 
        workspace_key = get_filename(uri)
        i = 2
        while workspace_key in workspace_keys:
            if i > 2:
                workspace_key = workspace_key[:-2] + "-" + str(i)
            else:
                workspace_key = workspace_key + "-" + str(i)
        workspace_keys.append(workspace_key)
        result.append((result_uri, os.path.join(workspace_uri, workspace_key)))

    return result

if __name__ == "__main__":
    from pyspark import SparkConf, SparkContext

    if len(sys.argv) != 2:
        print "ERROR: A single argument of the path to the request JSON is required"
        sys.exit(1)

    request_uri = sys.argv[1]
    parsed_request_uri = urlparse(request_uri)
    request = None
    if not parsed_request_uri.scheme:
        request = json.loads(open(request_uri).read())
        # read scheme from local path
    else:
        raise Error("Unimplemented Exception")
        # read scheme from S3

    print request
    images = request["images"]
    workspace = request["workspace"]
    jobId = request["jobId"]
    target = request["target"]

    gdal_uris_and_targets = process_uris(images, workspace)

    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)

    image_source_accumulator = sc.accumulator([], ImageSourceAccumulatorParam())

    def create_image_sources(uriElem, acc):
        (source_uri, imageFolder) = uriElem
        image_source = create_image_source(source_uri, imageFolder)
        acc += image_source
        return image_source

    uriRDD = sc.parallelize(gdal_uris_and_targets)
    image_sources = uriRDD.map(lambda uriElem: create_image_sources(uriElem, image_source_accumulator))
    chunk_tasks = image_sources.flatMap(lambda image_source: generate_chunk_tasks(image_source, CHUNK_SIZE))
    
    count = chunk_tasks.map(process_chunk_task).count()

    image_sources = image_source_accumulator.value
    print count
    print image_sources

    input = map(construct_image_info, image_sources)

    result = {
        "jobId": jobId,
        "target": target,
        "input": input
    }
        
    # Save off result
    workspace_parsed = urlparse(workspace)
    if not workspace_parsed.scheme:
        # Save to local files system
        open(os.path.join(workspace, OUTPUT_FILE_NAME), 'w').write(json.dumps(result))
    elif workspace_parsed.scheme == "s3":
        raise Error("Not implemented yet")

    print "Done."

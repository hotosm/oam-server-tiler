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
from rasterio import transform
from rasterio import crs
from rasterio import warp
from rasterio.warp import (reproject, RESAMPLING, calculate_default_transform)
from rasterio._io import virtual_file_to_buffer

APP_NAME = "Reproject and chunk"
CHUNK_SIZE = 1024
OUTPUT_FILE_NAME = "step1_result.json"

def get_filename(uri):
    return os.path.splitext(os.path.basename(uri))[0]

def mkdir_p(dir):
    try:
        os.makedirs(dir)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(dir):
            pass
        else: raise

ImageSource = namedtuple('ImageSource', "source_uri shape res bounds image_folder crs order")
ChunkTask = namedtuple('ChunkTask', "source_uri target_meta target")

def process_uris(images, workspace_uri):
    """
    Returns a list of tuples which is the GDAL-readable URI for each image,
    as well as a target workspace folder to place the chunked images.
    """
    result = []
    workspace_keys = []
    for (order, uri) in enumerate(images):
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
        result.append((result_uri, os.path.join(workspace_uri, workspace_key), order))

    return result

def create_image_source(source_uri, image_folder, order):
    with rasterio.drivers():
        with rasterio.open(source_uri) as src:
            shape = src.shape
            res = src.res
            bounds = src.bounds
            return ImageSource(source_uri=source_uri,
                               shape=shape,
                               res=res,
                               bounds=bounds,
                               image_folder=image_folder,
                               crs=src.crs,
                               order=order)

def generate_chunk_tasks(image_source, tile_dim):
    tasks = []
    
    (left, bottom, right, top) = (image_source.bounds.left,
                                  image_source.bounds.bottom,
                                  image_source.bounds.right,
                                  image_source.bounds.top)
    (width, height) = ((right - left), (top - bottom))
    (cols, rows) = image_source.shape
    tile_cols = cols / tile_dim
    tile_rows = rows / tile_dim
    tile_width = (1 / float(tile_cols)) * width
    tile_height = (1 / float(tile_rows)) * height

    def compute_target_meta(tile_col, tile_row):
        xmin = ((tile_col / float(tile_cols)) * width) + left
        ymax = top - (((tile_rows - (tile_row + 1)) / float(tile_rows)) * height)
        xmax = xmin + tile_width
        ymin = ymax - tile_height
        (target_affine, target_cols, target_rows) = calculate_default_transform(image_source.crs,
                                                                                "EPSG:3857",
                                                                                tile_dim,
                                                                                tile_dim,
                                                                                xmin,
                                                                                ymin,
                                                                                xmax,
                                                                                ymax)
        return {
            "transform": target_affine,
            "width": target_cols,
            "height": target_rows
        }

    for tile_row in range(0, tile_rows):
        for tile_col in range(0, tile_cols):
            target_meta = compute_target_meta(tile_row, tile_col)
            start_row = tile_row * tile_dim
            start_col = tile_col * tile_dim
            target_name = "%s-%d-%d.tif" % (get_filename(image_source.source_uri), tile_col, tile_row)
            target = os.path.join(image_source.image_folder, target_name)
            task = ChunkTask(source_uri=image_source.source_uri,
                             target_meta=target_meta,
                             target=target)
            tasks.append(task)

    return tasks

def process_chunk_task(task):
    """
    Chunks the image into tile_dim x tile_dim tiles,
    and saves them to the target folder (s3 or local)

    Returns the extent of the output raster.
    """

    creation_options = {
        "driver": "GTiff",
        "crs": "EPSG:3857",
        "tiled": False,
        "compress": "deflate",
        "predictor":   2, # 3 for floats, 2 otherwise
        "sparse_ok": True
    }

    bounds = None

    with rasterio.open(task.source_uri, "r") as src:
        meta = src.meta.copy()
        meta.update(creation_options)
        meta.update(task.target_meta)

        tmp_path = "/vsimem/" + get_filename(task.target)
#        tmp_path = "/vsimem/tile"

        with rasterio.open(tmp_path, "w", **meta) as tmp:
            bounds = tmp.bounds
            # Reproject the src dataset into image tile.
            for bidx in src.indexes:
                source = rasterio.band(src, bidx)
                destination = rasterio.band(tmp, bidx)

                warp.reproject(
                    source=source,
                    destination=destination,
                    resampling=RESAMPLING.bilinear
                )

            # check for chunks contain only NODATA
            tile_data = tmp.read()
            if tile_data.all() and tile_data[0][0][0] == src.nodata:
                return

    contents = bytearray(virtual_file_to_buffer(tmp_path))

    parsed_target = urlparse(task.target)

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
        output_path = task.target
        mkdir_p(os.path.dirname(output_path))

        with open(output_path, "w") as f:
            f.write(contents)

    return bounds

def construct_image_info(image_source):
    cellSize = { "width" : image_source.res[0], "height": image_source.res[1] }
    extent = { "xmin": image_source.bounds.left, "ymin": image_source.bounds.bottom,
               "xmax": image_source.bounds.right, "ymax": image_source.bounds.top }
    return {
        "cellSize" : cellSize,
        "extent" : extent,
        "images": image_source.image_folder
    }

def run_spark_job():
    from pyspark import SparkConf, SparkContext
    from pyspark.accumulators import AccumulatorParam

    class ImageSourceAccumulatorParam(AccumulatorParam):
        """
        Accumulator that will collect our image data that will be
        included as part of the input to the next stage of processing.
        """
        def zero(self, dummy):
            return []

        def addInPlace(self, sources1, sources2):
            res = []
            if sources1:
                res.extend(sources1)
            if sources2:
                res.extend(sources2)
            return res

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
        (source_uri, imageFolder, order) = uriElem
        image_source = create_image_source(source_uri, imageFolder, order)
        acc += [image_source]
        return image_source

    uriRDD = sc.parallelize(gdal_uris_and_targets)
    image_sources = uriRDD.map(lambda uriElem: create_image_sources(uriElem, image_source_accumulator))
    chunk_tasks = image_sources.flatMap(lambda image_source: generate_chunk_tasks(image_source, CHUNK_SIZE))
    
    count = chunk_tasks.map(process_chunk_task).count()

    image_sources = image_source_accumulator.value
    print "Processed %d images into %d chunks" % (len(image_sources), count)

    input = map(construct_image_info, sorted(image_sources, key=lambda im: im.order))

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

if __name__ == "__main__":
    run_spark_job()

    # # source_uri = "/Users/rob/proj/oam/data/postgis-gt-faceoff/raw/356f564e3a0dc9d15553c17cf4583f21-6.tif"
    # # image_folder = "/Users/rob/proj/oam/data/workspace/356f564e3a0dc9d15553c17cf4583f21-6"
    # source_uri = "/Users/rob/proj/oam/data/postgis-gt-faceoff/raw/LC81420412015111LGN00_bands_432.tif"
    # image_folder = "/Users/rob/proj/oam/data/workspace/LC81420412015111LGN00_bands_432"
    # image_source = create_image_source(source_uri, image_folder, 0)
    # chunk_tasks = generate_chunk_tasks(image_source, CHUNK_SIZE)
    # for task in chunk_tasks:
    #     #print task
    #     print process_chunk_task(task)
    # print construct_image_info(image_source)

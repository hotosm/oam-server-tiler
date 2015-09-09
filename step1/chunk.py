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

from affine import Affine

APP_NAME = "Reproject and chunk"
TILE_DIM = 256
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

ImageSource = namedtuple('ImageSource', "source_uri src_bounds src_shape src_crs zoom ll_bounds tile_bounds image_folder order")
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

def get_zoom(resolution, tile_dim):
    zoom = math.log((2 * math.pi * 6378137) / (resolution * tile_dim)) / math.log(2)
    if zoom - int(zoom) > 0.20:
        return int(zoom) + 1
    else:
        return int(zoom)

def create_image_source(source_uri, image_folder, order, tile_dim):
    with rasterio.drivers():
        with rasterio.open(source_uri) as src:
            shape = src.shape
            res = src.res
            bounds = src.bounds
            (ll_transform, ll_cols, ll_rows) = calculate_default_transform(src.crs,
                                                                           "EPSG:4326",
                                                                           src.shape[0],
                                                                           src.shape[1],
                                                                           src.bounds.left,
                                                                           src.bounds.bottom,
                                                                           src.bounds.right,
                                                                           src.bounds.top)
            w, n = ll_transform.xoff, ll_transform.yoff
            e, s = ll_transform * (ll_cols, ll_rows)
            ll_bounds = [w, s, e, n]

            (wm_transform, _, _) = calculate_default_transform(src.crs,
                                                               "EPSG:3857",
                                                               src.shape[0],
                                                               src.shape[1],
                                                               src.bounds.left,
                                                               src.bounds.bottom,
                                                               src.bounds.right,
                                                               src.bounds.top)

            resolution = max(abs(wm_transform[0]), abs(wm_transform[4]))
            zoom = get_zoom(resolution, tile_dim)
            min_tile = mercantile.tile(ll_bounds[0], ll_bounds[3], zoom)
            max_tile = mercantile.tile(ll_bounds[2], ll_bounds[1], zoom)
            
            return ImageSource(source_uri=source_uri,
                               src_bounds=src.bounds,
                               src_shape=src.shape,
                               src_crs=src.crs,
                               zoom=zoom,
                               ll_bounds=ll_bounds,
                               tile_bounds=[min_tile.x, min_tile.y, max_tile.x, max_tile.y],
                               image_folder=image_folder,
                               order=order)

def generate_chunk_tasks(image_source, tile_dim):
    tasks = []
    zoom = image_source.zoom
    (min_col, max_col) = (image_source.tile_bounds[0], image_source.tile_bounds[2])
    (min_row, max_row) = (image_source.tile_bounds[1], image_source.tile_bounds[3])

    for tile_col in range(min_col, min(max_col + 1, 2**zoom)):
        for tile_row in range(min_row, min(max_row + 1, 2**zoom)):
            tile_bounds = mercantile.bounds(tile_col, tile_row, zoom)
            (wm_left, wm_bottom, wm_right, wm_top)  = warp.transform_bounds("EPSG:4326",
                                                                           "EPSG:3857",
                                                                            tile_bounds.west,
                                                                            tile_bounds.south,
                                                                            tile_bounds.east,
                                                                            tile_bounds.north)
            affine = transform.from_bounds(wm_left, wm_bottom, wm_right, wm_top, tile_dim, tile_dim)
            target_meta = { 
                "transform": affine[:6],
                "width": tile_dim,
                "height": tile_dim 
            }

            target = os.path.join(image_source.image_folder, "%d/%d/%d.tif" % (zoom, tile_col, tile_row))

            # if target.endswith("356f564e3a0dc9d15553c17cf4583f21-24/18/193205/109909.tif"):
            #     print "HERRR: %s %s %s" % (image_source.source_uri, target, (wm_left, wm_bottom, wm_right, wm_top, tile_dim, tile_dim))
            #     raise Exception("yes")

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

    with rasterio.open(task.source_uri, "r") as src:
        meta = src.meta.copy()
        meta.update(creation_options)
        meta.update(task.target_meta)
        
        cols = meta["width"]
        rows = meta["height"]

        tmp_path = "/vsimem/" + get_filename(task.target)
        # tmp_path = "/vsimem/tile"
        # tmp_path = task.target

        with rasterio.open(tmp_path, "w", **meta) as tmp:
            # Reproject the src dataset into image tile.
            warped = []
            for bidx in src.indexes:
                source = rasterio.band(src, bidx)
                warped.append(numpy.zeros((cols, rows), dtype=meta['dtype']))

                warp.reproject(
                    source=source,
                    src_nodata=0,
                    destination=warped[bidx - 1],
                    dst_transform=meta["transform"],
                    dst_crs=meta["crs"],
                    resampling=RESAMPLING.bilinear
                )

            # check for chunks containing only zero values
            if not any(map(lambda b: b.any(), warped)):
                return

            # write out our warped data to the vsimem raster
            for bidx in src.indexes:
                tmp.write_band(bidx, warped[bidx - 1])

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

def construct_image_info(image_source):
    extent = { "xmin": image_source.ll_bounds[0], "ymin": image_source.ll_bounds[1],
               "xmax": image_source.ll_bounds[2], "ymax": image_source.ll_bounds[3] }

    gridBounds = { "colMin" : image_source.tile_bounds[0], "rowMin": image_source.tile_bounds[1],
                   "colMax": image_source.tile_bounds[2], "rowMax": image_source.tile_bounds[3] }
    return {
        "extent" : extent,
        "zoom" : image_source.zoom,
        "gridBounds" : gridBounds,
        "tiles": image_source.image_folder
    }

def run_spark_job(tile_dim):
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
        image_source = create_image_source(source_uri, imageFolder, order, tile_dim)
        acc += [image_source]
        return image_source

    uriRDD = sc.parallelize(gdal_uris_and_targets)
    image_sources = uriRDD.map(lambda uriElem: create_image_sources(uriElem, image_source_accumulator))
    chunk_tasks = image_sources.flatMap(lambda image_source: generate_chunk_tasks(image_source, tile_dim))
    chunks_count = chunk_tasks.cache().count()
    numPartitions = max(chunks_count / 100, min(50, len(gdal_uris_and_targets)))
    
    chunk_tasks.repartition(numPartitions).foreach(process_chunk_task)

    image_sources = image_source_accumulator.value
    print "Processed %d images into %d chunks" % (len(image_sources), chunks_count)

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
    tile_dim = TILE_DIM

    run_spark_job(tile_dim)

    # source_uri = "/Users/rob/proj/oam/data/postgis-gt-faceoff/raw/356f564e3a0dc9d15553c17cf4583f21-24.tif"
    # image_folder = "/Users/rob/proj/oam/data/workspace2/test"

    # # source_uri = "/Users/rob/proj/oam/data/postgis-gt-faceoff/raw/356f564e3a0dc9d15553c17cf4583f21-6.tif"
    # # image_folder = "/Users/rob/proj/oam/data/workspace/356f564e3a0dc9d15553c17cf4583f21-6"
    # source_uri = "/Users/rob/proj/oam/data/postgis-gt-faceoff/raw/LC81420412015111LGN00_bands_432.tif"
    # image_folder = "/Users/rob/proj/oam/data/workspace/LC81420412015111LGN00_bands_432"

    # image_source = create_image_source(source_uri, image_folder, 0, tile_dim)
    # chunk_tasks = generate_chunk_tasks(image_source, tile_dim)

    # for task in filter(lambda x: x.target.endswith('193205/109909.tif'), chunk_tasks):
    #     print task
    #     print process_chunk_task(task)
    # print construct_image_info(image_source)

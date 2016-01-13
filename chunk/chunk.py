import os, sys, shutil, traceback
import errno
import json
import itertools
import math
import multiprocessing
import tempfile
from urlparse import urlparse
from collections import namedtuple
from subprocess import call

import boto3
import mercantile
import numpy
import rasterio
from rasterio import transform
from rasterio import crs
from rasterio import warp
from rasterio.warp import (reproject, calculate_default_transform)
from rasterio._io import virtual_file_to_buffer

from affine import Affine

APP_NAME = "OAM Tiler Chunk"
TILE_DIM = 1024
OUTPUT_FILE_NAME = "step1_result.json"
STATUS_QUEUE = "https://sqs.us-east-1.amazonaws.com/670261699094/oam-server-tiler-status"
STATUS_QUEUE_REGION = "us-east-1"

def notify(m):
    client = boto3.client('sqs', region_name=STATUS_QUEUE_REGION)
    res = client.send_message(
        QueueUrl=STATUS_QUEUE,
        MessageBody=json.dumps(m)
    )

    if res['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception(json.dumps(res))

def notify_start(jobId):
    notify({ "jobId": jobId, "stage": "chunk", "status": "STARTED" })

def notify_success(jobId):
    notify({ "jobId": jobId, "stage": "chunk", "status": "FINISHED" })

def notify_failure(jobId, error_message, stack):
    notify({ "jobId": jobId, "stage": "chunk", "status": "FAILED", "error":  error_message, "stack": stack })


def create_tmp_directory(prefix):
    tmp = tempfile.mktemp(prefix=prefix, dir=os.path.join(os.environ['PWD'], "chunk-temp"))
    return makedirs_p(tmp)

def makedirs_p(d):
    if not os.path.exists(d):
        os.makedirs(d)
    return d

def get_local_copy(uri, local_dir):
    parsed = urlparse(uri)
    local_path = tempfile.mktemp(dir=local_dir)
    if parsed.scheme == "s3":
        cmd = ["aws", "s3", "cp", uri, local_path]
    elif parsed.scheme == "http":
        cmd = ["wget", "-O", local_path, uri]
    else:
        cmd = ["cp", uri, local_path]

    call(cmd)

    return local_path

def upload_to_working(local_src, dest):
    parsed = urlparse(dest)

    if parsed.scheme == "s3":
        cmd = ["aws", "s3", "cp", 
               "--acl", "public-read", 
               "--content-type", "image/tiff",
               local_src, dest]
    else:
        d = os.path.dirname(dest)
        if not os.path.exists(d):
            os.makedirs(d)
        cmd = ["cp", local_src, dest]

    call(cmd)

    return dest

def get_filename(uri):
    p = urlparse(uri)
    return os.path.splitext(os.path.join(p.netloc, p.path[1:]))[0]

def mkdir_p(dir):
    try:
        os.makedirs(dir)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(dir):
            pass
        else: raise

UriSet = namedtuple('UriSet', 'source_uri workspace_target workspace_source_uri image_folder order')
ImageSource = namedtuple('ImageSource', "origin_uri source_uri src_bounds src_shape src_crs zoom ll_bounds tile_bounds image_folder order")
ChunkTask = namedtuple('ChunkTask', "source_uri target_meta target")

def vsi_curlify(uri):
    """
    Creates a GDAL-readable path from the given URI
    """
    parsed = urlparse(uri)
    result_uri = ""
    if not parsed.scheme:
        result_uri = uri
    else:
        if parsed.scheme == "s3":
            result_uri = "/vsicurl/http://%s.s3.amazonaws.com%s" % (parsed.netloc, parsed.path)
        elif parsed.scheme.startswith("http"):
            result_uri = "/vsicurl/%s" % uri
        else:
            raise Exception("Unsupported scheme: %s" % parsed.schem)

    return result_uri

def write_bytes_to_target(target_uri, contents):
    parsed_target = urlparse(target_uri)
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
        output_path = target_uri
        mkdir_p(os.path.dirname(output_path))

        with open(output_path, "w") as f:
            f.write(contents)

def copy_tiles_to_workspace(source_uri, order, workspace_uri):
    # Download the file and retile
    results = []
    workspace_prefix = get_filename(source_uri)
    
    local_dir = create_tmp_directory(workspace_prefix)
    try :
        MAX_HEIGHT = 1024 * 2
        MAX_WIDTH = 1024 * 2

        local_path = get_local_copy(source_uri, local_dir)

        gdal_options = ["-of", "GTiff",
                        "-co", "compress=lzw",
                        "-co", "predictor=2",
                        "-co", "tiled=yes",
                        "-co", "blockxsize=512",
                        "-co", "blockysize=512"]

        # reproject
        reprojected_path = local_path + "-reprojected.tif"
        cmd = ["gdalwarp"] + gdal_options + ["-t_srs", "EPSG:3857",
                                             "-co", "BIGTIFF=YES", # Handle giant TIFFs.
                                             local_path,
                                             reprojected_path]
        call(cmd)

        # retile
        tiled_dir = local_path + "-tiled"
        os.mkdir(tiled_dir)
        cmd = ["gdal_retile.py"] + gdal_options + ["-ps", 
                                                   str(MAX_WIDTH), 
                                                   str(MAX_HEIGHT),
                                                   "-targetDir",
                                                   tiled_dir,
                                                   reprojected_path]
        call(cmd)

        tile_filenames = os.listdir(tiled_dir)

        workspace_basename = os.path.basename(workspace_prefix)
        reprojected_path_name = os.path.splitext(os.path.basename(reprojected_path))[0]

        # upload

        for tile_filename in tile_filenames:
            workspace_key = os.path.splitext(os.path.join(workspace_prefix, tile_filename.replace(reprojected_path_name, workspace_basename)))[0]
            workspace_target = os.path.join(workspace_uri, workspace_key + "-working.tif")
            upload_to_working(os.path.join(tiled_dir, tile_filename), workspace_target)

            workspace_source_uri = vsi_curlify(workspace_target)

            image_folder = os.path.join(workspace_uri, workspace_key)

            uri_set = UriSet(source_uri = source_uri,
                             workspace_target = workspace_target,
                             workspace_source_uri = workspace_source_uri,
                             image_folder = image_folder,
                             order = order)

            results.append(uri_set)

        shutil.rmtree(local_dir)
    finally:
        if local_dir:
            shutil.rmtree(local_dir, ignore_errors=True)

    return results

def get_zoom(resolution, tile_dim):
    zoom = math.log((2 * math.pi * 6378137) / (resolution * tile_dim)) / math.log(2)
    if zoom - int(zoom) > 0.20:
        return int(zoom) + 1
    else:
        return int(zoom)

def create_image_source(origin_uri, source_uri, image_folder, order, tile_dim):
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
            
            return ImageSource(origin_uri=origin_uri,
                               source_uri=source_uri,
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
        "tiled": True,
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
                    # resampling=RESAMPLING.bilinear
                )

            # check for chunks containing only zero values
            if not any(map(lambda b: b.any(), warped)):
                return False

            # write out our warped data to the vsimem raster
            for bidx in src.indexes:
                tmp.write_band(bidx, warped[bidx - 1])

    contents = bytearray(virtual_file_to_buffer(tmp_path))

    write_bytes_to_target(task.target, contents)
    return True

def construct_image_info(image_source):
    extent = { "xmin": image_source.ll_bounds[0], "ymin": image_source.ll_bounds[1],
               "xmax": image_source.ll_bounds[2], "ymax": image_source.ll_bounds[3] }

    gridBounds = { "colMin" : image_source.tile_bounds[0], "rowMin": image_source.tile_bounds[1],
                   "colMax": image_source.tile_bounds[2], "rowMax": image_source.tile_bounds[3] }
    return {
        "sourceUri": image_source.origin_uri,
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

    request_uri = sys.argv[1]

    # If there's more arguements, its to turn off notifications
    publish_notifications = True
    if len(sys.argv) == 3:
        publish_notifications = False

    parsed_request_uri = urlparse(request_uri)
    request = None
    if not parsed_request_uri.scheme:
        request = json.loads(open(request_uri).read())
    else:
        client = boto3.client("s3")
        o = client.get_object(Bucket=parsed_request_uri.netloc, Key=parsed_request_uri.path[1:])
        request = json.loads(o["Body"].read())

    source_uris = request["images"]
    workspace = request["workspace"]
    jobId = request["jobId"]
    target = request["target"]

    if publish_notifications:
        notify_start(jobId)

    try:
        conf = SparkConf().setAppName(APP_NAME)
        sc = SparkContext(conf=conf)


        for x in os.environ:
            print "%s: %s" % (x, os.environ[x])

        uri_sets = sc.parallelize(enumerate(source_uris)).flatMap(lambda (o, i): copy_tiles_to_workspace(i, o, workspace))
        source_tile_count = uri_sets.cache().count()

        image_source_accumulator = sc.accumulator([], ImageSourceAccumulatorParam())

        def create_image_sources(uri_set, acc):
            image_source = create_image_source(uri_set.source_uri, uri_set.workspace_source_uri, uri_set.image_folder, uri_set.order, tile_dim)
            acc += [image_source]
            return image_source

        image_sources = uri_sets.repartition(source_tile_count).map(lambda uri_set: create_image_sources(uri_set, image_source_accumulator))
        chunk_tasks = image_sources.flatMap(lambda image_source: generate_chunk_tasks(image_source, tile_dim))
        chunks_count = chunk_tasks.cache().count()
        numPartitions = max(chunks_count / 10, min(50, source_tile_count))

        valid_source_uris = set(chunk_tasks.repartition(numPartitions).filter(process_chunk_task).map(lambda task: task.source_uri).distinct().collect())

        image_sources = filter(lambda ims: ims.source_uri in valid_source_uris, image_source_accumulator.value)
        print "Processed %d image tiles into approximately %d chunks" % (len(image_sources), chunks_count)

        input_info = map(construct_image_info, sorted(image_sources, key=lambda im: im.order))

        result = {
            "jobId": jobId,
            "target": target,
            "tileSize": tile_dim,
            "input": input_info
        }

        # Save off result
        workspace_parsed = urlparse(workspace)
        if not workspace_parsed.scheme:
            # Save to local files system
            open(os.path.join(workspace, OUTPUT_FILE_NAME), 'w').write(json.dumps(result))
        elif workspace_parsed.scheme == "s3":
            client = boto3.client("s3")

            bucket = workspace_parsed.netloc
            key = os.path.join(workspace_parsed.path, OUTPUT_FILE_NAME)[1:]

            client.put_object(Bucket=bucket, Key=key, Body=json.dumps(result))
    except Exception, e:
        if publish_notifications:
            notify_failure(jobId, "%s: %s" % (type(e).__name__, e.message), traceback.format_exc(1000))
        raise

    if publish_notifications:
        notify_success(jobId)

    print "Done."

if __name__ == "__main__":
    tile_dim = TILE_DIM

    run_spark_job(tile_dim)

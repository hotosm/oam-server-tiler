# OAM Tiler

This code is meant to run through Amazon EMR as a two step process for tiling.

### Chunk

The `chunk.py` is a pyspark application that runs code which takes a set of images and converts them into 1024 by 1024 tiled GeoTiffs
according to the mercator tile zoom level that matches the source image's resolution closest.

There is a single input argument, which is the local or s3 path to a JSON file that specifies the request. The request JSON looks like this:

```javascript
{
    "jobId": "emr-test-job-full",
    "target": "s3://oam-tiles/oam-tiler-test-emr-full",
    "images": [
        "http://hotosm-oam.s3.amazonaws.com/356f564e3a0dc9d15553c17cf4583f21-12.tif",
        "http://oin-astrodigital.s3.amazonaws.com/LC81420412015111LGN00_bands_432.TIF"
    ],
    "workspace": "s3://workspace-oam-hotosm-org/emr-test-job-full"
}
```

In this input, we see that we have two input images. The order of the images in the `images` array will determine the priority of the images
in the later mosaic step; images that have a lower index (higher on the list) will take priority over images that have a higher index (lower on the list). So in this
example, the pixels of `356f564e3a0dc9d15553c17cf4583f21-12.tif` would be overlayed on top of the pixels of `LC81420412015111LGN00_bands_432.TIF` (`356f564e3a0dc9d15553c17cf4583f21-12.tif` would be "above" `LC81420412015111LGN00_bands_432.TIF`).

This set of chunked imagery is dumped into a working folder inside an S3 bucket, determined by the `workspace` field in the input JSON.

Also placed in that folder is a `step1_result.json` object that looks like the following:

```javascript
{
    "jobId": "emr-test-job-full",
    "target": "s3://oam-tiles/oam-tiler-test-emr-full",
    "tileSize": 1024,
    "input": [
        {
            "gridBounds": {
                "rowMax": 27449,
                "colMax": 48278,
                "colMin": 48269,
                "rowMin": 27438
            },
            "tiles": "s3://workspace-oam-hotosm-org/emr-test-job-full/356f564e3a0dc9d15553c17cf4583f21-12",
            "zoom": 16,
            "extent": {
                "xmin": 85.1512716462415,
                "ymin": 28.028055072893892,
                "ymax": 28.079387757442355,
                "xmax": 85.20296835795914
            }
        },
        {
            "gridBounds": {
                "rowMax": 434,
                "colMax": 753,
                "colMin": 746,
                "rowMin": 427
            },
            "tiles": "s3://workspace-oam-hotosm-org/emr-test-job-full/LC81420412015111LGN00_bands_432.TIF",
            "zoom": 10,
            "extent": {
                "xmin": 82.52879450382254,
                "ymin": 26.359556467334755,
                "ymax": 28.486769014205997,
                "xmax": 84.86307565466431
            }
        }
    ]
}
```

### Mosaic

The Mosaic step is a Scala Spark application that reads in the GeoTiffs and result information from the previous step and performs the mosaicing.
It outputs a set of web mercator `z/x/y` 256 x 256 PNG tiles to the target s3 or local directory.

## Running

There are shell scripts that will run this against EM in the root of the repository. It uses the `awscli` tool to run EMR commands.

## Timing Notes:

Running full set:
python: 19m
scala:   4m

With 1024:
python: 13m
scala:   5m

With tmp files and copy:
python: 8m

With tmp files:
python: 14m (8.8m max task)

With just copy:
python: 9 min


About 4 cents, 40 cents for workers, 4 cents for master. Tiling job for under 50 cents.
9.5 G of imagery.

## Notes on EMR instances:

13 minutes for processing (not counting EMR deployment time, which is about 15 minutes for spot instances)
10 m3.xlarge worker nodes and 1 m3.xlarge master at spot prices (around 5 cents an hour)
32 images totalling 9.5 GB

## Notes:

Chunker (step1) generates the reprojected image, in web mercator tiles that are 1024 x 1024.
It passes through the extent, resolution and tile layout.

The Tiler generates an object that, given all images resolution, extents and layouts, can create
filters per image that will allow us to filter only those tiles that require tiling at a specific
zoom level.


__Still seems about 150 feet to the left of where it should be (digital globe imagery)__

python chunk time:	14m38.559s
                    14m34.361s

scala pyramid:   7m54.779s

# Introduction

OVDS-Utils is a python library implementing classes and wrapers with easier to comprehend interfaces to [openvds](https://community.opengroup.org/osdu/platform/domain-data-mgmt-services/seismic/open-vds).

## Examples are:
 * VDS class implementing ``__getitem__`` method for easy data read out and many others feature to make life easier.

## To install:

Run ``pip install ovds-utils`` or execute ``python setup.py install`` in the source directory


## Creating and reading VDS source example

You can easily create and access the VDS source/file by simply creating a VDS class instance and using ``__getitem__`` method to read data.

```python
import numpy as np

from ovds_utils.ovds.enums import BrickSizes
from ovds_utils.vds import VDS

shape = (251, 51, 126)
data = np.random.rand(*shape).astype(np.float32)

vds = VDS(
    path="example.vds",
    connection_string="",
    shape=shape,
    data=data,
    databrick_size=BrickSizes.BrickSize_128
)

print(vds[:10,0,0])
>>> [0.14836921 0.06490713 0.05770212 0.2364456  0.49000826 0.1573576
 0.5017615  0.456749   0.6573513  0.72831243]
```
## Writing to VDS source chunk by chunk

```python
import numpy as np

from ovds_utils.metadata import MetadataTypes, MetadataValue
from ovds_utils.ovds.enums import BrickSizes, Formats
from ovds_utils.vds import VDS, Channel, Components, Formats

metadata = {
    "example": MetadataValue(value="value", category="category#1", type=MetadataTypes.String)
}

shape = (251, 51, 126)
data = np.random.rand(*shape).astype(np.float64)
zeros = np.zeros(shape, dtype=np.float64)

vds = VDS(
    "example.vds",
    data=zeros,
    metadata_dict=metadata,
    databrick_size=BrickSizes.BrickSize_64,
    channels=[
        Channel(
            name="Channel0",
            format=Formats.R64,
            unit="unitless",
            value_range_min=0.0,
            value_range_max=1000.0,
            components=Components.Components_1
        )
    ]
)
for c in vds.channel(0).chunks():
    c[:, :, :] = data[c.slices]
    c.release()
vds.channel(0).commit()

print(vds[:10,0,0])
>>> [0.14836921 0.06490713 0.05770212 0.2364456  0.49000826 0.1573576
 0.5017615  0.456749   0.6573513  0.72831243]
```
## Links
* https://pypi.org/project/ovds-utils/
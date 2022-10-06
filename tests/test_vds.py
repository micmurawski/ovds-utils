import os
from tempfile import TemporaryDirectory

import numpy as np

from ovds_utils.metadata import MetadataTypes, MetadataValue
from ovds_utils.ovds.enums import BrickSizes, Formats
from ovds_utils.vds import VDS, Channel, Components, Axis


def test_vds_shape():
    shape = (251, 51, 126)
    data = np.random.rand(*shape).astype(np.float32)
    axis = [
        Axis(
            samples=s,
            name=f"X{i}",
            unit="unitless",
            coordinate_max=100.0,
            coordinate_min=-100.0
        )
        for i, s in enumerate(shape)
    ]
    with TemporaryDirectory() as dir:
        vds = VDS(
            os.path.join(dir, "example.vds"),
            "",
            channels=[
                Channel(
                    name="Amplitude",
                    format=Formats.R64,
                    unit="unitless",
                    value_range_min=0.0,
                    value_range_max=1000.0,
                    components=Components.Components_1
                )
            ],
            axis=axis,
            channels_data=[data],
            databrick_size=BrickSizes.BrickSize_128
        )
        assert vds[:, :, :].shape == vds.shape == shape
        for _ in range(shape[0]):
            assert all(
                np.array_equal(data[i, 0, :], vds[i, 0, :])
                for i in range(shape[0])
            )


# def test_create_vds_by_chunks():
#    shape = (251, 51, 126)
#    dtype = np.float64
#    data = np.random.rand(*shape).astype(dtype)
#    metadata = {
#        "example": MetadataValue(value="value", category="category#1", type=MetadataTypes.String)
#    }
#    data = np.random.rand(*shape).astype(dtype)
#    zeros = np.zeros(shape, dtype=dtype)
#
#    with TemporaryDirectory() as dir:
#        with TemporaryDirectory() as dir:
#            vds = VDS(
#                os.path.join(dir, "example.vds"),
#                data=zeros,
#                metadata_dict=metadata,
#                databrick_size=BrickSizes.BrickSize_64,
#                channels=[
#                    Channel(
#                        name="Channel0",
#                        format=Formats.R64,
#                        unit="unitless",
#                        value_range_min=0.0,
#                        value_range_max=1000.0,
#                        components=Components.Components_1
#                    )
#                ]
#            )
#            for chunk in vds.channel(0).chunks():
#                chunk[:, :, :] = data[chunk.slices]
#                chunk.release()
#            vds.channel(0).commit()
#
#
# def test_vds_3d_cube_default_axis_name():
#    """tests if default axis descriptors preserve naming convention in case of 3D cubes"""
#    shape = (251, 51, 126)
#    data = np.random.rand(*shape).astype(np.float32)
#    with TemporaryDirectory() as dir:
#        vds = VDS(
#            os.path.join(dir, "example.vds"),
#            "",
#            shape=shape,
#            data=data,
#            databrick_size=BrickSizes.BrickSize_128
#        )
#        vds.axis_descriptors[0] == ("Sample", "unitless", 126)
#        vds.axis_descriptors[1] == ("Crossline", "unitless", 51)
#        vds.axis_descriptors[2] == ("Inline", "unitless", 251)
#

import os
from tempfile import TemporaryDirectory

import numpy as np

from ovds_utils.metadata import MetadataTypes, MetadataValue
from ovds_utils.ovds.enums import BrickSizes, Formats
from ovds_utils.vds import VDS, Axis, Channel, Components, AccessModes


def test_vds_shape():
    shape = (251, 51, 126)
    data = np.random.rand(*shape).astype(np.float32)
    names = ["Sample", "Crossline", "Inline"]
    axes = [
        Axis(
            samples=s,
            name=names[i],
            unit="unitless",
            coordinate_max=1000.0,
            coordinate_min=-1000.0
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
                    format=Formats.R32,
                    unit="unitless",
                    value_range_min=0.0,
                    value_range_max=1.0,
                    components=Components._1
                )
            ],
            axes=axes,
            channels_data=[
                data
            ],
            databrick_size=BrickSizes._64,
            access_mode=AccessModes.Create
        )
        assert vds[:, :, :].shape == vds.shape == shape
        for _ in range(shape[0]):
            assert all(
                np.array_equal(data[i, 0, :5], vds[i, 0, :5])
                for i in range(shape[0])
            )


def test_create_vds_by_chunks():
    shape = (251, 51, 126)
    dtype = np.float64
    data = np.random.rand(*shape).astype(dtype)
    metadata = {
        "example": MetadataValue(value="value", category="category#1", type=MetadataTypes.String)
    }
    data = np.random.rand(*shape).astype(dtype)
    names = ["Sample", "Crossline", "Inline"]
    axes = [
        Axis(
            samples=s,
            name=names[i],
            unit="unitless",
            coordinate_max=1000.0,
            coordinate_min=-1000.0
        )
        for i, s in enumerate(shape)
    ]
    with TemporaryDirectory() as dir:
        vds = VDS(
            os.path.join(dir, "example.vds"),
            metadata_dict=metadata,
            axes=axes,
            databrick_size=BrickSizes._64,
            channels=[
                Channel(
                    name="Channel0",
                    format=Formats.R64,
                    unit="unitless",
                    value_range_min=0.0,
                    value_range_max=1000.0,
                    components=Components._1
                )
            ],
            access_mode=AccessModes.Create
        )
        for chunk in vds.channel(0).chunks():
            chunk[:, :, :] = data[chunk.slices]
            chunk.release()
        vds.channel(0).commit()

        assert vds[:, :, :].shape == vds.shape == shape
        for _ in range(shape[0]):
            assert all(
                np.array_equal(data[i, 0, :], vds[i, 0, :])
                for i in range(shape[0])
            )


def test_vds_3d_cube_default_axis_name():
    """tests if default axis descriptors preserve naming convention in case of 3D cubes"""
    shape = (251, 51, 126)
    data = np.random.rand(*shape).astype(np.float32)
    names = ["Sample", "Crossline", "Inline"]
    axes = [
        Axis(
            samples=s,
            name=names[i],
            unit="unitless",
            coordinate_max=1000.0,
            coordinate_min=-1000.0
        )
        for i, s in enumerate(shape)
    ]
    with TemporaryDirectory() as dir:
        vds = VDS(
            os.path.join(dir, "example.vds"),
            "",
            axes=axes,
            channels_data=[
                data
            ],
            channels=[
                Channel(
                    name="Amplitude",
                    format=Formats.R32,
                    unit="unitless",
                    value_range_min=0.0,
                    value_range_max=1.0,
                    components=Components._1
                )
            ],
            databrick_size=BrickSizes._128,
            access_mode=AccessModes.Create
        )
        vds.axis_descriptors[0] == ("Sample", "unitless", 126)
        vds.axis_descriptors[1] == ("Crossline", "unitless", 51)
        vds.axis_descriptors[2] == ("Inline", "unitless", 251)


def test_vds_3d_cube_initialize_and_wirte_later():
    shape = (251, 51, 126)
    dtype = np.float64
    data = np.random.rand(*shape).astype(dtype)
    metadata = {
        "example": MetadataValue(value="value", category="category#1", type=MetadataTypes.String)
    }
    data = np.random.rand(*shape).astype(dtype)
    names = ["Sample", "Crossline", "Inline"]
    axes = [
        Axis(
            samples=s,
            name=names[i],
            unit="unitless",
            coordinate_max=1000.0,
            coordinate_min=-1000.0
        )
        for i, s in enumerate(shape)
    ]
    with TemporaryDirectory() as dir:
        with VDS(
            os.path.join(dir, "example.vds"),
            metadata_dict=metadata,
            axes=axes,
            databrick_size=BrickSizes._64,
            channels=[
                Channel(
                    name="Channel0",
                    format=Formats.R64,
                    unit="unitless",
                    value_range_min=0.0,
                    value_range_max=1000.0,
                    components=Components._1
                )
            ],
            access_mode=AccessModes.Create
        ) as vds:
            pass

        with VDS(
            os.path.join(dir, "example.vds"),
            access_mode=AccessModes.ReadWrite
        ) as vds:
            for chunk in vds.channel(0).chunks():
                chunk[:, :, :] = data[chunk.slices]
                chunk.release()
            vds.channel(0).commit()

        with VDS(
            os.path.join(dir, "example.vds"),
            access_mode=AccessModes.ReadOnly
        ) as vds:
            assert vds[:, :, :].shape == vds.shape == shape
            for _ in range(shape[0]):
                assert all(
                    np.array_equal(data[i, 0, :], vds[i, 0, :])
                    for i in range(shape[0])
                )

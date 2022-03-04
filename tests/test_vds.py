import os
from tempfile import TemporaryDirectory

import numpy as np

from ovds_utils.ovds.enums import BrickSizes, Formats
from ovds_utils.vds import VDS


def test_vds_shape():
    shape = (251, 51, 126)
    data = np.random.rand(*shape).astype(np.float32)
    with TemporaryDirectory() as dir:
        vds = VDS(
            os.path.join(dir, "example.vds"),
            "",
            shape=shape,
            data=data,
            databrick_size=BrickSizes.BrickSize_128
        )
        assert vds[:, :, :].shape == vds.shape == shape
        for _ in range(shape[0]):
            assert all(
                np.array_equal(data[i, 0, :], vds[i, 0, :])
                for i in range(shape[0])
            )


def test_create_vds_by_chunks():
    shape = (251, 51, 126)
    dtype = np.float64
    data = np.random.rand(*shape).astype(dtype)
    with TemporaryDirectory() as dir:
        shape = (251, 51, 126)
        data = np.random.rand(*shape).astype(dtype)
        zeros = np.zeros(shape, dtype=dtype)
        with TemporaryDirectory() as dir:
            vds = VDS(
                os.path.join(dir, "example.vds"),
                "",
                shape=shape,
                data=zeros,
                format=Formats.R64,
                databrick_size=BrickSizes.BrickSize_64,
            )
            for chunk in vds.channel(0).chunks():
                chunk[:, :, :] = data[chunk.slices]
                chunk.release()
            vds.channel(0).commit()

import os
from tempfile import TemporaryDirectory

import numpy as np

from ovds_utils.ovds.enums import BrickSizes
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

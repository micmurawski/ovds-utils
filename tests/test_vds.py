import os
from tempfile import TemporaryDirectory

from ovds_utils.vds import VDS
from ovds_utils.ovds.enums import BrickSizes


def test_vds_shape():
    shape = (251, 51, 126)
    with TemporaryDirectory() as dir:
        vds = VDS(
            os.path.join(dir, "example.vds"),
            "",
            shape=shape,
            databrick_size=BrickSizes.BrickSize_128
        )
        assert vds[:, :, :].shape == vds.shape == shape

import json
from subprocess import PIPE, Popen
from typing import Any, AnyStr, Dict, Sequence

import openvds
from humanfriendly import format_size

from ovds_utils.logging import get_logger

logger = get_logger(__name__)


METADATATYPE2GETFUNCTION = {
    "MetadataType.IntVector2": openvds.core.VolumeDataLayout.getMetadataIntVector2,
    "MetadataType.Float": openvds.core.VolumeDataLayout.getMetadataFloat,
    "MetadataType.DoubleVector2": openvds.core.VolumeDataLayout.getMetadataDoubleVector2,
    "MetadataType.String": openvds.core.VolumeDataLayout.getMetadataString,
    "MetadataType.Int": openvds.core.VolumeDataLayout.getMetadataInt,
    "MetadataType.Double": openvds.core.VolumeDataLayout.getMetadataDouble,
    "MetadataType.BLOB": openvds.core.VolumeDataLayout.getMetadataBLOB,
}


METADATATYPE2SETFUNCTION = {
    "MetadataType.IntVector2": openvds.core.MetadataContainer.setMetadataIntVector2,
    "MetadataType.Float": openvds.core.MetadataContainer.setMetadataFloat,
    "MetadataType.DoubleVector2": openvds.core.MetadataContainer.setMetadataDoubleVector2,
    "MetadataType.String": openvds.core.MetadataContainer.setMetadataString,
    "MetadataType.Int": openvds.core.MetadataContainer.setMetadataInt,
    "MetadataType.Double": openvds.core.MetadataContainer.setMetadataDouble,
    "MetadataType.BLOB": openvds.core.MetadataContainer.setMetadataBLOB,
}


def get_bricksize_values(brick_size: openvds.core.VolumeDataLayoutDescriptor.BrickSize, shape: Sequence[int]):
    return [2 ** brick_size.value if s >= 2 ** brick_size.value else s for s in shape]


def get_element_size(format: openvds.core.VolumeDataFormat, components: openvds.core.VolumeDataComponents):
    if format == openvds.VolumeDataChannelDescriptor.Format.Format_1Bit:
        return 1
    elif format == openvds.VolumeDataChannelDescriptor.Format.Format_U8:
        return 1 * components.value
    elif format == openvds.VolumeDataChannelDescriptor.Format.Format_U16:
        return 2 * components.value
    elif format in (
        openvds.VolumeDataChannelDescriptor.Format.Format_R32,
        openvds.VolumeDataChannelDescriptor.Format.Format_U32,
    ):
        return 4 * components.value
    elif format in (
        openvds.VolumeDataChannelDescriptor.Format.Format_U64,
        openvds.VolumeDataChannelDescriptor.Format.Format_R64,
    ):
        return 8 * components.value
    else:
        raise Exception("Illegal format")


def check_block_size(
    brickSize: openvds.core.VolumeDataLayoutDescriptor.BrickSize,
    channels: int,
    shape: Sequence[int],
    format: openvds.core.VolumeDataFormat,
    components: openvds.core.VolumeDataComponents
):
    element_size = get_element_size(format, components)
    brick_size_values = get_bricksize_values(brickSize, shape)

    datablock_size = brick_size_values[0]
    for i in range(1, len(shape)):
        datablock_size *= brick_size_values[i]
    datablock_size *= channels * element_size
    logger.info(f"Datablock size: {format_size(datablock_size)}")
    logger.info(f"Estimated process size: {format_size(datablock_size*10)}")
    if datablock_size > 2147483647:
        raise Exception(
            f"Datablock is too big ({brick_size_values[0]} x {brick_size_values[1]}\
                 x {brick_size_values[2]} x {channels} x {element_size} bytes)"
        )


def copy_ovds_metadata(metadata_details: Dict[AnyStr, Any], result_metadata_container: openvds.core.MetadataContainer):
    for k, v in metadata_details.items():
        set_method = METADATATYPE2SETFUNCTION[v.type]
        set_method(result_metadata_container, v.category, k, v.value)


def get_vdsinfo_bin() -> str:
    """retrives absolute path to VDSInfo based on openvds package"""
    from pathlib import Path

    import openvds as vds_package

    root_init_vds = Path(vds_package.__file__)
    assert root_init_vds.exists()
    assert root_init_vds.is_file()

    # build binary dir e.g. venv/bin
    bin_directory = root_init_vds.parent / ".." / ".." / ".." / ".." / "bin"
    assert bin_directory.exists()
    assert bin_directory.is_dir()

    # look for VDSInfo inside bin dir
    vdsinfo_bin = bin_directory / "VDSInfo"
    assert vdsinfo_bin.exists()
    assert vdsinfo_bin.is_file()

    return str(vdsinfo_bin.resolve().absolute())


def get_vds_info(path: AnyStr, connection_string: AnyStr):
    vds_bin = get_vdsinfo_bin()

    if connection_string:
        cmd = f'{vds_bin} {path} --connection "{connection_string}"'
    else:
        cmd = f"{vds_bin} {path} {connection_string}"
    process = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = process.communicate()
    try:
        return json.loads(out)
    except Exception as e:
        logger.error(out)
        logger.error(err)
        raise Exception(f"Could not parse VDSInfo {e}")

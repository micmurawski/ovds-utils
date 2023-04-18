from typing import Any, AnyStr, Dict, List

import numpy as np
import openvds

from .enums import InitValue
from .utils import copy_ovds_metadata

FORMAT2NPTYPE = {
    openvds.VolumeDataChannelDescriptor.Format.Format_R64: np.float64,
    openvds.VolumeDataChannelDescriptor.Format.Format_R32: np.float32,
    openvds.VolumeDataChannelDescriptor.Format.Format_U8: np.uint8,
    openvds.VolumeDataChannelDescriptor.Format.Format_U16: np.uint16,
    openvds.VolumeDataChannelDescriptor.Format.Format_U32: np.uint32,
    openvds.VolumeDataChannelDescriptor.Format.Format_U64: np.uint64
}


def write_pages(
    accessor: openvds.core.VolumeDataPageAccessor, data: np.array,
    format: openvds.VolumeDataChannelDescriptor.Format
):
    dtype = FORMAT2NPTYPE[format]
    for c in range(accessor.getChunkCount()):
        page = accessor.createPage(c)
        buf = np.array(page.getWritableBuffer(), copy=False, dtype=dtype)
        (min, max) = page.getMinMax()
        buf[:, :, :] = data[
            min[2]: max[2],
            min[1]: max[1],
            min[0]: max[0],
        ]
        page.release()
    accessor.commit()


def write_nan_pages(
    accessor: openvds.core.VolumeDataPageAccessor,
    format: openvds.VolumeDataChannelDescriptor.Format
):
    dtype = FORMAT2NPTYPE[format]
    for c in range(accessor.getChunkCount()):
        page = accessor.createPage(c)
        buf = np.array(page.getWritableBuffer(), copy=False, dtype=dtype)
        buf[:, :, :] = np.full(buf.shape, np.nan, dtype=dtype)
        page.release()
    accessor.commit()


def write_zero_pages(
    accessor: openvds.core.VolumeDataPageAccessor,
    format: openvds.VolumeDataChannelDescriptor.Format
):
    dtype = FORMAT2NPTYPE[format]
    for c in range(accessor.getChunkCount()):
        page = accessor.createPage(c)
        buf = np.array(page.getWritableBuffer(), copy=False, dtype=dtype)
        buf[:, :, :] = np.zeros(buf.shape, dtype=dtype)
        page.release()
    accessor.commit()


INITVALUE = {
    InitValue.NaN: write_nan_pages,
    InitValue.zero: write_zero_pages
}


def create_vds_attributes(
    databrick_size: openvds.core.VolumeDataLayoutDescriptor.BrickSize,
    metadata_dict: Dict[AnyStr, Any],
    lod_levels: openvds.VolumeDataLayoutDescriptor.LODLevels,
    options: openvds.VolumeDataLayoutDescriptor.Options,
    negative_margin: int,
    positive_margin: int,
    brick_size_2d_multiplier: int,
    full_resolution_dimension: int,
    channles,
    axes
):
    layout_descriptor = openvds.VolumeDataLayoutDescriptor(
        brickSize=databrick_size,
        lodLevels=lod_levels,
        brickSize2DMultiplier=brick_size_2d_multiplier,
        options=options,
        negativeMargin=negative_margin,
        positiveMargin=positive_margin,
        fullResolutionDimension=full_resolution_dimension,
    )
    metadata_container = openvds.MetadataContainer()
    copy_ovds_metadata(metadata_dict, metadata_container)
    axis_descriptors = []
    for a in axes[::-1]:
        axis_descriptors.append(
            openvds.VolumeDataAxisDescriptor(
                a.samples,
                a.name,
                a.unit,
                a.coordinate_min,
                a.coordinate_max
            )
        )

    channel_descriptors = []
    for c in channles:
        channel_descriptors.append(
            openvds.VolumeDataChannelDescriptor(
                format=c.format.value,
                components=c.components.value,
                name=c.name,
                unit=c.unit,
                valueRangeMin=c.value_range_min,
                valueRangeMax=c.value_range_max
            )
        )
    return layout_descriptor, axis_descriptors, channel_descriptors, metadata_container


def create_vds(
    path: AnyStr,
    connection_string: AnyStr,
    metadata_dict: Dict[AnyStr, Any],
    channels: List,
    axes: List,
    negative_margin: int,
    positive_margin: int,
    databrick_size: openvds.VolumeDataLayoutDescriptor.BrickSize,
    access_mode: openvds.IVolumeDataAccessManager.AccessMode,
    lod: openvds.VolumeDataLayoutDescriptor.LODLevels,
    options: int,
    brick_size_2d_multiplier: int,
    full_resolution_dimension: int,
    default_max_pages: int = 8,
    channels_data=None,
    init_value: InitValue = InitValue.zero,
    close=True
):
    (
        layout_descriptor,
        axis_descriptors,
        channel_descriptors,
        metadata_container
    ) = create_vds_attributes(
        databrick_size=databrick_size,
        metadata_dict=metadata_dict,
        channles=channels,
        axes=axes,
        lod_levels=lod,
        negative_margin=negative_margin,
        positive_margin=positive_margin,
        options=options,
        brick_size_2d_multiplier=brick_size_2d_multiplier,
        full_resolution_dimension=full_resolution_dimension,
    )

    (
        layout_descriptor,
        axis_descriptors,
        channel_descriptors,
        metadata_container
    ) = create_vds_attributes(
        databrick_size=databrick_size,
        metadata_dict=metadata_dict,
        channles=channels,
        axes=axes,
        lod_levels=lod,
        negative_margin=negative_margin,
        positive_margin=positive_margin,
        options=options,
        brick_size_2d_multiplier=brick_size_2d_multiplier,
        full_resolution_dimension=full_resolution_dimension,
    )
    vds = openvds.create(
        url=path,
        connectionString=connection_string,
        layoutDescriptor=layout_descriptor,
        axisDescriptors=axis_descriptors,
        channelDescriptors=channel_descriptors,
        metadata=metadata_container,
    )
    access_manager = openvds.getAccessManager(vds)
    if channels_data:
        for i, data in enumerate(channels_data):
            channel = channels[i]
            accessor = access_manager.createVolumeDataPageAccessor(
                dimensionsND=channel.dimensions_nd.value,
                accessMode=access_mode,
                lod=lod,
                channel=i,
                maxPages=default_max_pages,
            )
            write_pages(accessor, data, channel.format.value)
    else:
        for i in range(len(channels)):
            channel = channels[i]
            accessor = access_manager.createVolumeDataPageAccessor(
                dimensionsND=channel.dimensions_nd.value,
                accessMode=access_mode,
                lod=lod,
                channel=i,
                maxPages=default_max_pages,
            )
            INITVALUE[init_value](accessor, channel.format.value)

    if close:
        openvds.close(vds)
    return vds

from typing import Any, AnyStr, Dict, List

import numpy as np
import openvds

from .utils import check_block_size, copy_ovds_metadata

FORMAT2FLOAT = {
    openvds.VolumeDataChannelDescriptor.Format.Format_R64: np.float64,
    openvds.VolumeDataChannelDescriptor.Format.Format_R32: np.float32
}


def write_pages(
    accessor: openvds.core.VolumeDataPageAccessor, data: np.array,
    format: openvds.VolumeDataChannelDescriptor.Format
):
    dtype = FORMAT2FLOAT[format]
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
    axis
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
    for a in axis[::-1]:
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
    axis: List,
    negative_margin: int,
    positive_margin: int,
    dimensions_nd: openvds.DimensionsND,
    databrick_size: openvds.VolumeDataLayoutDescriptor.BrickSize,
    access_mode: openvds.IVolumeDataAccessManager.AccessMode,
    components: openvds.VolumeDataChannelDescriptor.Components,
    format: openvds.VolumeDataChannelDescriptor.Format,
    lod: openvds.VolumeDataLayoutDescriptor.LODLevels,
    options: int,
    brick_size_2d_multiplier: int,
    full_resolution_dimension: int,
    default_max_pages: int = 8,
    channels_data=None,
    close=True
):
    shape = [a.samples for a in axis]
    check_block_size(databrick_size, 1, shape, format, components)

    (
        layout_descriptor,
        axis_descriptors,
        channel_descriptors,
        metadata_container
    ) = create_vds_attributes(
        databrick_size=databrick_size,
        metadata_dict=metadata_dict,
        channles=channels,
        axis=axis,
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
        axis=axis,
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
            accessor = access_manager.createVolumeDataPageAccessor(
                dimensionsND=dimensions_nd,
                accessMode=access_mode,
                lod=lod,
                channel=i,
                maxPages=default_max_pages,
            )
            write_pages(accessor, data, format)

    if close:
        openvds.close(vds)
    return vds

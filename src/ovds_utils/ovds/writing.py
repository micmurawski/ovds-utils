from typing import Any, AnyStr, Dict, Sequence

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


def write_zero_pages(
    accessor: openvds.core.VolumeDataPageAccessor,
    format: openvds.VolumeDataChannelDescriptor.Format
):
    dtype = FORMAT2FLOAT[format]
    for c in range(accessor.getChunkCount()):
        page = accessor.createPage(c)
        buf = np.array(page.getWritableBuffer(), copy=False, dtype=dtype)
        buf[:, :, :] = np.zeros(buf.shape, dtype=dtype)
        page.release()
    accessor.commit()


def write_empty_pages(accessor: openvds.core.VolumeDataPageAccessor, number_of_workers: int):
    chunks_count = accessor.getChunkCount()
    result = {}
    ranges = []
    merged_ranges = []
    interval = 0

    # find interval - chunks laying next to each other
    for c in range(chunks_count):
        page = accessor.createPage(c)
        (min, max) = page.getMinMax()
        ranges.append((min[:-3], max[:-3]))
        buf = np.array(page.getWritableBuffer(), copy=False)
        buf[:, :, :] = np.reshape(np.array([0.0] * buf.size), buf.shape)
        page.release()
    accessor.commit()

    for c in range(chunks_count):
        if c == 0:
            interval += 1
        elif ranges[0][0][0] == ranges[interval][0][0]:
            break
        else:
            interval += 1

    # check if power of interval is not interval itself
    cnt = 0
    while True:
        new_interval = interval ** (2 + cnt)
        if new_interval == chunks_count:
            interval = interval ** (2 + cnt - 1)
            break
        elif chunks_count % new_interval == 0:
            cnt += 1
        else:
            interval = interval ** (2 + cnt - 1)
            break

    # merge cubes next to each other
    for i in range(len(ranges)):
        if i == 0:
            merged_ranges.append(
                (
                    ranges[0][0],
                    ranges[interval - 1][1],
                )
            )
        elif i % interval == 0:
            merged_ranges.append(
                (
                    ranges[i][0],
                    ranges[i + interval - 1][1],
                )
            )

    val = len(merged_ranges) // number_of_workers

    last = 0
    for i in range(number_of_workers):
        if i == number_of_workers - 1:
            result[i] = {
                "range": (merged_ranges[0][0], merged_ranges[-1][1]),
                "chunks": (
                    last,
                    chunks_count,
                ),
            }
        else:
            poped_ranges = [merged_ranges.pop(0) for i in range(val)]
            result[i] = {
                "range": (poped_ranges[0][0], poped_ranges[-1][1]),
                "chunks": (
                    last,
                    (i + val) * interval,
                ),
            }
            last = (i + val) * interval

    return result


def create_vds_attributes_from_vds_info(
    vds_info: Dict[AnyStr, Any],
    metadata_dict: Dict[AnyStr, Any] = {},
    shape: Sequence[int] = None,
    databrick_size: openvds.core.VolumeDataLayoutDescriptor.BrickSize = None,
    begin: Sequence[int] = None,
    end: Sequence[int] = None
):
    if begin and end:
        shape = (
            end[0] - begin[0],
            end[1] - begin[1],
            end[2] - begin[2],
        )
    layout_descriptor = openvds.VolumeDataLayoutDescriptor(
        getattr(
            openvds.VolumeDataLayoutDescriptor.BrickSize,
            vds_info["layoutInfo"]["layoutDescriptor"]["brickSize"],
        )
        if databrick_size is None
        else databrick_size,
        vds_info["layoutInfo"]["layoutDescriptor"]["negativeMargin"],
        vds_info["layoutInfo"]["layoutDescriptor"]["positiveMargin"],
        vds_info["layoutInfo"]["layoutDescriptor"]["brickSize2DMultiplier"],
        getattr(
            openvds.VolumeDataLayoutDescriptor.LODLevels,
            vds_info["layoutInfo"]["layoutDescriptor"]["lodLevels"],
        ),
        openvds.VolumeDataLayoutDescriptor.Options.Options_None,
        1
        if vds_info["layoutInfo"]["layoutDescriptor"][
            "forceFullResolutionDimension"
        ]
        else 0,
    )
    if begin is end is None:
        axis_descriptors = [
            openvds.VolumeDataAxisDescriptor(
                v["numSamples"],
                v["name"],
                v["unit"],
                v["coordinateMin"],
                v["coordinateMax"],
            )
            for v in vds_info["layoutInfo"]["axisDescriptors"]
        ]
    else:
        axis_descriptors = []
        axis_steps = [
            (v["coordinateMax"] - v["coordinateMin"]) / v["numSamples"]
            for v in vds_info["layoutInfo"]["axisDescriptors"]
        ]
        for i in range(len(vds_info["layoutInfo"]["axisDescriptors"])):
            v = vds_info["layoutInfo"]["axisDescriptors"][i]
            axis_descriptors.append(
                openvds.VolumeDataAxisDescriptor(
                    shape[::-1][i],
                    v["name"],
                    v["unit"],
                    v["coordinateMin"] + (begin[i] * axis_steps[i]),
                    v["coordinateMin"] + (end[i] * axis_steps[i]),
                )
            )
        channel_descriptors = [
            openvds.VolumeDataChannelDescriptor(
                getattr(openvds.VolumeDataChannelDescriptor.Format, v["format"]),
                getattr(
                    openvds.VolumeDataChannelDescriptor.Components, v["components"]
                ),
                v["name"],
                v["unit"],
                v["valueRange"][0],
                v["valueRange"][1],
                # 0.0,
                # ((shape[2] * 3) * (shape[1] * 2) * shape[0]) - 1.0,
            )
            for v in vds_info["layoutInfo"]["channelDescriptors"]
        ]

    metadata_container = openvds.MetadataContainer()
    copy_ovds_metadata(metadata_dict, metadata_container)

    return layout_descriptor, axis_descriptors, channel_descriptors, metadata_container


def create_default_vds_attributes(
    databrick_size: openvds.core.VolumeDataLayoutDescriptor.BrickSize,
    metadata_dict: Dict[AnyStr, Any],
    shape: Sequence[int],
    components: openvds.core.VolumeDataComponents,
    format: openvds.core.VolumeDataFormat,
    lod_levels=openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_None,
    brick_size_2d_multiplier: int = 4,
    options=openvds.VolumeDataLayoutDescriptor.Options.Options_None,
    negative_margin: int = 0,
    positive_margin: int = 0,
    full_resolution_dimension: int = 0,
    channles=None
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

    for i, size in enumerate(shape):
        axis_descriptors.append(
            openvds.VolumeDataAxisDescriptor(
                size,
                f"X{i}",
                "unitless",
                -1000.0,
                1000.0,
            )
        )
    if channles is None:
        channel_descriptors = [
            openvds.VolumeDataChannelDescriptor(
                format=format,
                components=components,
                name="Channel0",
                unit="unitless",
                valueRangeMin=0.0,
                valueRangeMax=1000.0,
            )
        ]
    else:
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
    shape: Sequence[int],
    metadata_dict: Dict[AnyStr, Any] = {},
    vds_info: Dict[AnyStr, Any] = None,
    begin: Sequence[int] = None,
    end: Sequence[int] = None,
    channels=None,
    databrick_size: openvds.VolumeDataLayoutDescriptor.BrickSize = openvds.VolumeDataLayoutDescriptor.BrickSize.BrickSize_128,  # NOQA
    access_mode: openvds.IVolumeDataAccessManager.AccessMode = openvds.IVolumeDataAccessManager.AccessMode.AccessMode_Create,  # NOQA
    components: openvds.VolumeDataChannelDescriptor.Components = openvds.VolumeDataChannelDescriptor.Components.Components_1,  # NOQA
    format: openvds.VolumeDataChannelDescriptor.Format = openvds.VolumeDataChannelDescriptor.Format.Format_R32,  # NOQA
    data=None,
    close=True
):
    if shape:
        shape = shape[::-1]

    if begin and end:
        shape = (
            end[2] - begin[2],
            end[1] - begin[1],
            end[0] - begin[0],
        )
    check_block_size(databrick_size, 1, shape, format, components)
    if vds_info is None:
        (
            layout_descriptor,
            axis_descriptors,
            channel_descriptors,
            metadata_container
        ) = create_default_vds_attributes(
            databrick_size=databrick_size,
            metadata_dict=metadata_dict,
            shape=shape,
            components=components,
            format=format,
            channles=channels
        )
    else:
        (
            layout_descriptor,
            axis_descriptors,
            channel_descriptors,
            metadata_container
        ) = create_vds_attributes_from_vds_info(
            vds_info=vds_info,
            metadata_dict=metadata_dict,
            shape=shape,
            begin=begin,
            end=end,
            databrick_size=databrick_size
        )

    vds = openvds.create(
        path,
        connection_string,
        layout_descriptor,
        axis_descriptors,
        channel_descriptors,
        metadata_container,
    )
    access_manager = openvds.getAccessManager(vds)
    accessor = access_manager.createVolumeDataPageAccessor(
        dimensionsND=openvds.DimensionsND.Dimensions_012,
        accessMode=access_mode,
        lod=0,
        channel=0,
        maxPages=8,
        chunkMetadataPageSize=1024,
    )

    if data is None:
        write_zero_pages(accessor, format)
    else:
        write_pages(accessor, data, format)

    if close:
        openvds.close(vds)
    return vds

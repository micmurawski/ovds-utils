from itertools import product
from math import ceil
from typing import List, Sequence, Tuple

import openvds


def key2gen(key: Sequence[slice], shape: Tuple[int]):
    return product(*[range(k.start or 0, k.stop or s, k.step or 1) for k, s in zip(key, shape)])


def key2shape(key: Sequence[slice], shape: Tuple[int]):
    return tuple(ceil(((k.stop or s)-(k.start or 0))/(k.step or 1)) for k, s in zip(key, shape))


def get_sample_position(coord_tuple, axis_descriptors: List[openvds.core.VolumeDataAxisDescriptor]):
    """transforms coordinate tuple to sample position"""
    return tuple(
        [
            axis_descriptors[idx].coordinateToSamplePosition(coord)
            for idx, coord in enumerate(coord_tuple)
        ]
    )


def get_volume_sample(key, vds, lod=0, channel_idx=0, interpolation_method=openvds.core.InterpolationMethod.Linear):
    layout = openvds.getLayout(vds)
    axis_descriptors = [
        layout.getAxisDescriptor(dim) for dim in range(layout.getDimensionality())
    ]
    shape = tuple(int(a.numSamples) for a in axis_descriptors)[::-1]
    _shape = key2shape(key, shape)
    gen = key2gen(key, shape)
    accessManager = openvds.VolumeDataAccessManager(vds)
    channelDescriptors = [
        layout.getChannelDescriptor(dim) for dim in range(layout.getChannelCount())
    ]
    channel = channelDescriptors[0]
    positions = [get_sample_position(p, axis_descriptors) for p in gen]
    req = accessManager.requestVolumeSamples(
        positions,
        lod=lod,
        replacementNoValue=channel.getNoValue(),
        channel=channel_idx,
        interpolationMethod=interpolation_method,
    )
    return req.data.reshape(_shape)

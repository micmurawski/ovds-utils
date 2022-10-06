from enum import Enum

import openvds


class Formats(Enum):
    _1Bit = openvds.VolumeDataChannelDescriptor.Format.Format_1Bit
    U8 = openvds.VolumeDataChannelDescriptor.Format.Format_U8
    U16 = openvds.VolumeDataChannelDescriptor.Format.Format_U16
    R32 = openvds.VolumeDataChannelDescriptor.Format.Format_R32
    U32 = openvds.VolumeDataChannelDescriptor.Format.Format_U32
    U64 = openvds.VolumeDataChannelDescriptor.Format.Format_U64
    R64 = openvds.VolumeDataChannelDescriptor.Format.Format_R64


class Components(Enum):
    Components_1 = openvds.VolumeDataChannelDescriptor.Components.Components_1
    Components_2 = openvds.VolumeDataChannelDescriptor.Components.Components_2
    Components_4 = openvds.VolumeDataChannelDescriptor.Components.Components_4


class BrickSizes(Enum):
    BrickSize_64 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_64
    BrickSize_128 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_128
    BrickSize_256 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_256
    BrickSize_512 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_512
    BrickSize_1024 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_1024
    BrickSize_2048 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_2048
    BrickSize_4096 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_4096


class AccessModes(Enum):
    Create = openvds.IVolumeDataAccessManager.AccessMode.AccessMode_Create
    ReadOnly = openvds.IVolumeDataAccessManager.AccessMode.AccessMode_ReadOnly
    ReadWrite = openvds.IVolumeDataAccessManager.AccessMode.AccessMode_ReadWrite


class Dimensions(Enum):
    Dimensions_012 = openvds.DimensionsND.Dimensions_012


class LOD(Enum):
    _0 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_None
    _1 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_1
    _2 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_2
    _3 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_3
    _4 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_4
    _5 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_5
    _6 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_6
    _7 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_7
    _8 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_8
    _9 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_9
    _10 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_10
    _11 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_11
    _12 = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_12


class MetadataTypes(Enum):
    IntVector2 = "MetadataType.IntVector2"
    Float = "MetadataType.Float"
    DoubleVector2 = "MetadataType.DoubleVector2"
    String = "MetadataType.String"
    Int = "MetadataType.Int"
    Double = "MetadataType.Double"
    BLOB = "MetadataType.BLOB"

from enum import Enum, auto

import openvds


class InitValue(Enum):
    NaN = auto()
    zero = auto()


class Formats(Enum):
    _1Bit = openvds.VolumeDataChannelDescriptor.Format.Format_1Bit
    U8 = openvds.VolumeDataChannelDescriptor.Format.Format_U8
    U16 = openvds.VolumeDataChannelDescriptor.Format.Format_U16
    R32 = openvds.VolumeDataChannelDescriptor.Format.Format_R32
    U32 = openvds.VolumeDataChannelDescriptor.Format.Format_U32
    U64 = openvds.VolumeDataChannelDescriptor.Format.Format_U64
    R64 = openvds.VolumeDataChannelDescriptor.Format.Format_R64


class Components(Enum):
    _1 = openvds.VolumeDataChannelDescriptor.Components.Components_1
    _2 = openvds.VolumeDataChannelDescriptor.Components.Components_2
    _4 = openvds.VolumeDataChannelDescriptor.Components.Components_4


class BrickSizes(Enum):
    _64 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_64
    _128 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_128
    _256 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_256
    _512 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_512
    _1024 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_1024
    _2048 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_2048
    _4096 = openvds.core.VolumeDataLayoutDescriptor.BrickSize.BrickSize_4096


class AccessModes(Enum):
    Create = openvds.IVolumeDataAccessManager.AccessMode.AccessMode_Create
    ReadOnly = openvds.IVolumeDataAccessManager.AccessMode.AccessMode_ReadOnly
    ReadWrite = openvds.IVolumeDataAccessManager.AccessMode.AccessMode_ReadWrite


class Dimensions(Enum):
    _012 = openvds.DimensionsND.Dimensions_012


class Options(Enum):
    _None = openvds.VolumeDataLayoutDescriptor.Options.Options_None
    _2DLODs = openvds.VolumeDataLayoutDescriptor.Options.Options_Create2DLODs


class LOD(Enum):
    _None = openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_None
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

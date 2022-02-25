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


class MetadataTypes(Enum):
    IntVector2 = "MetadataType.IntVector2"
    Float = "MetadataType.Float"
    DoubleVector2 = "MetadataType.DoubleVector2"
    String = "MetadataType.String"
    Int = "MetadataType.Int"
    Double = "MetadataType.Double"
    BLOB = "MetadataType.BLOB"

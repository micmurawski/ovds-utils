from copy import deepcopy
from typing import AnyStr, List, Sequence, Tuple, Union

import numpy as np
import openvds

from ovds_utils.exceptions import VDSException
from ovds_utils.logging import get_logger
from ovds_utils.metadata import MetadataContainer
from ovds_utils.ovds import (LOD, AccessModes, BrickSizes, Components, Dimensions, Formats, InitValue, Options,
                             create_vds)
from ovds_utils.ovds.utils import get_vds_info
from ovds_utils.ovds.writing import FORMAT2NPTYPE

logger = get_logger(__name__)


class VDSChunk:
    def __init__(
        self,
        number: int,
        accessor: openvds.core.VolumeDataPageAccessor,
        format: Formats
    ) -> None:
        super().__init__()
        self.is_released = False
        self.number = number
        self.accesor = accessor
        self._page = None
        self.format = format

    def __repr__(self) -> str:
        return f"<VDSChunk(number={self.number})>"

    def __getitem__(self, key: Sequence[Union[int, slice]]) -> np.array:
        dtype = FORMAT2NPTYPE[self.format.value]
        page = self.accesor.readPage(self.number)
        buf = np.array(page.getBuffer(), copy=False, dtype=dtype)
        return buf.__getitem__(key)

    def __setitem__(self, key: Sequence[Union[int, slice]], value: np.array):
        dtype = FORMAT2NPTYPE[self.format.value]
        buf = np.array(self.page.getWritableBuffer(), copy=False, dtype=dtype)
        return buf.__setitem__(key, value)

    def release(self) -> None:
        self.page.release()
        self.is_released = True

    @property
    def page(self):
        if self._page is None:
            try:
                self._page = self.accesor.createPage(self.number)
            except openvds.core.InvalidOperation as e:
                if e.args[0] == "Cannot create a page that already exists":
                    self._page = self.accesor.readPage(self.number)
                else:
                    raise e
        return self._page

    @property
    def minmax(self) -> Tuple[Sequence[int]]:
        _min, _max = self.page.getMinMax()
        _min = _min[:3][::-1]
        _max = _max[:3][::-1]
        return _min, _max

    @property
    def slices(self) -> Tuple[slice]:
        return tuple(slice(i, j) for i, j in zip(*self.minmax))


class VDSChunksGenerator:
    def __init__(
        self,
        chunks_count: int,
        accessor: openvds.core.VolumeDataPageAccessor,
        format: Formats
    ) -> None:
        self.chunks_count = chunks_count
        self.accessor = accessor
        self.format = format

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self):
        if self.n < self.chunks_count:
            chunk = VDSChunk(
                number=self.n, accessor=self.accessor, format=self.format
            )
            self.n += 1
            return chunk
        else:
            raise StopIteration


class Axis:
    def __init__(
        self,
        samples: int,
        name: AnyStr,
        coordinate_min: float,
        coordinate_max: float,
        unit: AnyStr = "unitless",
    ) -> None:
        self.samples = samples
        self.name = name
        self.unit = unit
        self.coordinate_min = coordinate_min
        self.coordinate_max = coordinate_max

    def __repr__(self) -> str:
        return f"<Axis(name={self.name}, unit={self.unit}, range=({self.coordinate_min}, {self.coordinate_max}))>"


class Channel:
    def __init__(
            self,
            name: AnyStr,
            format: Formats,
            unit: AnyStr,
            value_range_min: float,
            value_range_max: float,
            components: Components,
            accessor: openvds.core.VolumeDataPageAccessor = None,
            chunks_count: int = None,
            vds_source=None,
            shape: Sequence[int] = None,
            dimensions_nd=Dimensions._012,
    ) -> None:
        self._vds_source = vds_source
        self.name = name
        self.format = format
        self.unit = unit
        self.accessor = accessor
        self.chunks_count = chunks_count
        self.shape = shape
        self.components = components
        self.value_range_min = value_range_min
        self.value_range_max = value_range_max
        self.dimensions_nd = dimensions_nd

    def __repr__(self) -> str:
        return f"<Channel(name={self.name}, unit={self.unit}, format={self.format.name})>"

    def chunks(self) -> VDSChunksGenerator:
        return VDSChunksGenerator(chunks_count=self.chunks_count, accessor=self.accessor, format=self.format)

    def get_chunk(self, number: int) -> VDSChunk:
        if number not in set(range(self.chunks_count)):
            raise VDSException(f"Chunk number is out of range of: 0 to {self.chunks_count-1}")
        return VDSChunk(
            number=number, accessor=self.accessor, format=self.format
        )

    def _read_data(
        self,
        vds_source: openvds.core.VDS,
        begin: Sequence[int],
        end: Sequence[int],
        lod: int = 0,
        replacementNoValue: float = 0.0,
        channel: int = 0
    ):
        begin = begin[::-1] + ([0]*len(begin))
        end = end[::-1] + ([1]*len(end))

        dims = (
            end[2] - begin[2],
            end[1] - begin[1],
            end[0] - begin[0],
        )
        accessManager = openvds.VolumeDataAccessManager(vds_source)
        req = accessManager.requestVolumeSubset(
            begin,  # start slice
            end,  # end slice
            format=self.format.value,
            lod=lod,
            replacementNoValue=replacementNoValue,
            channel=channel,
        )

        if req.data is None:
            err_code, err_msg = accessManager.getCurrentDownloadError()
            logger.exception(err_code)
            logger.exception(err_msg)
            raise RuntimeError(f"requestVolumeSubset failed! Message: {err_msg}, Error Code: {err_code}")

        return req.data.reshape(*dims)

    def _getitem_for_whole_dataset(self, key: Sequence[Union[int, slice]]) -> np.array:
        is_int = False
        if all([isinstance(i, int) for i in key]):
            begin = [i for i in key]
            end = [i+1 for i in key]
            return self._read_data(self._vds_source, begin, end).__getitem__(tuple(0 for i in key))
        elif all([isinstance(i, int) or isinstance(i, slice) for i in key]):
            begin = []
            end = []
            for i, k in enumerate(key):
                if isinstance(k, slice):
                    begin.append(k.start if k.start else 0)
                    end.append(k.stop if k.stop else self.shape[i])
                elif isinstance(k, int):
                    is_int = True
                    begin.append(k)
                    end.append(k+1)
        else:
            raise VDSException("Item key is not list of slices or int")

        if is_int:
            return self._read_data(self._vds_source, begin, end).__getitem__(
                tuple(
                    slice(None, None) if isinstance(i, slice) else 0 for i in key
                )
            )
        else:
            return self._read_data(self._vds_source, begin, end)

    def __getitem__(self, key: Sequence[Union[int, slice]]) -> np.array:
        return self._getitem_for_whole_dataset(key)

    def commit(self):
        self.accessor.commit()


class VDS:
    def __init__(
        self,
        path: AnyStr,
        connection_string: AnyStr = "",
        databrick_size: BrickSizes = BrickSizes._128,
        metadata_dict: MetadataContainer = {},
        channels_data: List[np.array] = None,
        channels: List[Channel] = None,
        axes: List[Axis] = None,
        lod: LOD = LOD._None,
        negative_margin: int = 0,
        positive_margin: int = 0,
        options: Options = Options._None,
        full_resolution_dimension: int = 0,
        brick_size_2d_multiplier: int = 4,
        init_value: InitValue = InitValue.zero
    ) -> None:
        super().__init__()
        self.path = path
        self.connection_string = connection_string
        self._channels = {}
        self._axes = {}

        try:
            self._vds_source = openvds.open(path, connection_string)
            vds_info = get_vds_info(path, connection_string)
        except RuntimeError as e:
            if str(e) in ("Open error: File::open "):
                logger.debug("Creating new VDS source...")
                self.create(
                    path=path,
                    connection_string=connection_string,
                    databrick_size=databrick_size,
                    channels=channels,
                    axes=axes,
                    metadata_dict=metadata_dict,
                    channels_data=channels_data,
                    access_mode=AccessModes.Create,
                    lod=lod,
                    positive_margin=positive_margin,
                    negagitve_margin=negative_margin,
                    options=options,
                    full_resolution_dimension=full_resolution_dimension,
                    brick_size_2d_multiplier=brick_size_2d_multiplier,
                    init_value=init_value
                )
                self = self.__init__(
                    path=path,
                    connection_string=connection_string,
                    databrick_size=databrick_size,
                    metadata_dict=metadata_dict,
                )
                return
            else:
                raise VDSException(f"Open VDS resulted with: {str(e)}") from e

        self._layout = openvds.getLayout(self._vds_source)
        self._dimensionality = self._layout.getDimensionality()

        vds_info = get_vds_info(path, connection_string)
        _info = vds_info['layoutInfo'] if 'layoutInfo' in vds_info else vds_info

        for i, j in enumerate(_info['axisDescriptors']):
            self._axes[j['name']] = Axis(
                samples=j['numSamples'],
                name=j['name'],
                unit=j['unit'],
                coordinate_max=j['coordinateMax'],
                coordinate_min=j['coordinateMin']
            )
        self.chunks_count = self.count_number_of_chunks(self.shape, databrick_size)
        for i, j in enumerate(_info['channelDescriptors']):
            self._channels[j['name']] = Channel(
                vds_source=self._vds_source,
                shape=self.shape,
                components=getattr(Components, j['components'].replace("Components", "")),
                name=j['name'],
                unit=j['unit'],
                format=getattr(
                    Formats, j['format'].replace("Format_", "")
                ),
                value_range_max=j['valueRange'][1],
                value_range_min=j['valueRange'][0],
                accessor=self._create_accessor(channel=i),
                chunks_count=self.count_number_of_chunks(self.shape, databrick_size)
            )

    def channel(self, number: int) -> Channel:
        return self.channels[number]

    @property
    def channels(self) -> List[Channel]:
        return list(self._channels.values())

    @property
    def axes(self) -> List[Axis]:
        return list(self._axes.values())[::-1]

    def get_channel(self, name: AnyStr) -> Channel:
        return self._channels[name]

    def __del__(self):
        openvds.close(self._vds_source)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        return

    @property
    def metadata(self) -> MetadataContainer:
        return MetadataContainer.get_from_layout(self._layout)

    @property
    def negative_margin(self) -> int:
        return self._layout.getLayoutDescriptor().getNegativeMargin()

    @property
    def positive_margin(self) -> int:
        return self._layout.getLayoutDescriptor().getPositiveMargin()

    @property
    def full_resolution_dimension(self) -> int:
        return self._layout.getLayoutDescriptor().fullResolutionDimension

    @property
    def options(self):
        name = str(self._layout.getLayoutDescriptor().getOptions()).replace("Options.Options", "")
        return getattr(Options, name)

    @property
    def brick_size_2d_multiplier(self) -> int:
        return self._layout.getLayoutDescriptor().brickSizeMultiplier2D

    @property
    def databrick_size(self) -> BrickSizes:
        name = str(self._layout.getLayoutDescriptor().getBrickSize()).replace("BrickSize.BrickSize", "")
        return getattr(BrickSizes, name)

    @property
    def lod(self) -> LOD:
        name = str(self._layout.getLayoutDescriptor().getLODLevels()).replace("LODLevels.LODLevels", "")
        return getattr(LOD, name)

    def __str__(self) -> str:
        return f"<{self.__class__.__qualname__}(path={self.path})>"

    @ property
    def axis_descriptors(self):
        return tuple((
            self._axes[k].name,
            self._axes[k].unit,
            self._axes[k].samples
        ) for k in self._axes)[::-1]

    @ property
    def shape(self):
        return tuple(self._axes[k].samples for k in self._axes)[::-1]

    @ staticmethod
    def create(
        path: AnyStr,
        connection_string: AnyStr,
        databrick_size: BrickSizes,
        access_mode: AccessModes,
        channels: List[Channel],
        axes: List[Axis],
        positive_margin: int,
        negagitve_margin: int,
        full_resolution_dimension: int,
        brick_size_2d_multiplier: int,
        lod: LOD,
        options: Options,
        init_value: InitValue,
        metadata_dict: MetadataContainer = None,
        channels_data: List[np.array] = None,
    ):
        return create_vds(
            path=path,
            connection_string=connection_string,
            metadata_dict=metadata_dict,
            channels=channels,
            axes=axes,
            databrick_size=databrick_size.value,
            access_mode=access_mode.value,
            lod=lod.value,
            channels_data=channels_data,
            negative_margin=negagitve_margin,
            positive_margin=positive_margin,
            options=options.value,
            brick_size_2d_multiplier=brick_size_2d_multiplier,
            full_resolution_dimension=full_resolution_dimension,
            init_value=init_value,
            close=True
        )

    @property
    def accessor(self):
        if self._accessor is None:
            raise VDSException("Accessor was not initialized use create_accessor method.")
        return self._accessor

    def _create_accessor(
            self,
            access_mode: AccessModes = AccessModes.ReadWrite,
            lod: int = 0,
            channel: int = 0,
            maxPages: int = 8,
            chunkMetadataPageSize: int = 1024,
            dimensionsND=Dimensions._012,
    ):
        access_manager = openvds.getAccessManager(self._vds_source)
        accessor = access_manager.createVolumeDataPageAccessor(
            dimensionsND=dimensionsND.value,
            accessMode=access_mode.value,
            lod=lod,
            channel=channel,
            maxPages=maxPages,
            chunkMetadataPageSize=chunkMetadataPageSize,
        )
        self._accessor = accessor
        return self._accessor

    @staticmethod
    def count_number_of_chunks(shape: int, brick_size: BrickSizes):
        x = [round(i/(2**brick_size.value.value)) for i in shape]
        r = 1
        for i in x:
            if i != 0:
                r *= i
        return r

    def __getitem__(self, key: Sequence[Union[int, slice]]) -> np.array:
        return self.channel(0).__getitem__(key)


class VDSComposite:
    def __init__(self, subsets: Sequence[VDS] = None, slice_dim: int = None) -> None:
        if subsets is None:
            subsets = []
        if slice_dim is None:
            slice_dim = 0
        self.__subsets = subsets
        self.__shapes = []

        for s in self.__subsets:
            self.__shapes.append(s.shape)

        self.__slice_dim = slice_dim

        self.shape = None
        for i, s in enumerate(subsets):
            if i == 0:
                self.shape = list(s.shape)
            else:
                self.shape[slice_dim] += s.shape[slice_dim]

    def add_subset(self, subset: VDS):
        self.__subsets.append(subset)
        self.__shapes.append(subset.shape)

        self.shape = None
        for i, s in enumerate(self.__subsets):
            if i == 0:
                self.shape = list(s.shape)
            else:
                self.shape[self.__slice_dim] += s.shape[self.__slice_dim]

        self.shape = tuple(self.shape)

    def __getitem__(self, key: Sequence[Union[int, slice]]) -> np.array:
        is_int = False
        if all([isinstance(i, slice) or isinstance(i, int) for i in key]):
            if all([isinstance(i, int) for i in key]):
                cnt = 0
                _slice = key[self.__slice_dim]
                key = list(key)
                for i in range(len(self.__shapes)):
                    b = cnt + self.__shapes[i][self.__slice_dim]
                    if _slice >= cnt and _slice < b:
                        key[self.__slice_dim] = _slice - cnt
                        return self.__subsets[i].__getitem__(tuple(key))
                    cnt += self.__shapes[i][self.__slice_dim]
            else:
                _key = []
                for i, k in enumerate(key):
                    if isinstance(k, int):
                        is_int = True
                        _key.append(slice(k, k+1))
                    else:
                        _key.append(slice(k.start or 0, k.stop or 0, k.step))
                _key = tuple(_key)
        else:
            raise VDSException("Key elements must be instances of slice or int.")

        for i, k in enumerate(_key):
            if k.start is not None and (k.start > self.shape[i] + 1 or k.stop > self.shape[i] + 1):
                raise Exception(f"{_key} is out of range {self.shape}")

        _slice = _key[self.__slice_dim]
        keys = []
        a = 0
        for i in range(len(self.__shapes)):
            b = a + self.__shapes[i][0]
            if _slice.start < b and (_slice.start == _slice.stop):
                _slices = list(deepcopy(_key))
                _slices[self.__slice_dim] = slice(0, _slice.stop - a)
                keys.append(tuple(_slices))
                break
            elif _slice.start < b:
                _slices = list(deepcopy(_key))
                _slices[self.__slice_dim] = slice(
                    0,
                    self.__shapes[i][self.__slice_dim]
                )
                keys.append(tuple(_slices))
            else:
                keys.append(None)
            if _slice.stop < b:
                _slices = list(deepcopy(keys[-1]))
                _slices[self.__slice_dim] = slice(0, _slice.stop - a)
                keys[-1] = tuple(_slices)
                break
            a += self.__shapes[i][0]
        result_list = []
        for i, s in enumerate(keys):
            if s:
                result_list.append(self.__subsets[i].__getitem__(s))
        if is_int:
            return np.concatenate(tuple(result_list), axis=self.__slice_dim).__getitem__(
                tuple(
                    slice(None, None) if isinstance(i, slice) else 0 for i in key
                )
            )

        return np.concatenate(tuple(result_list), axis=self.__slice_dim)

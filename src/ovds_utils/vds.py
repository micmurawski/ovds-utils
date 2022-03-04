from copy import deepcopy
from typing import AnyStr, List, Sequence, Tuple, Union

import numpy as np
import openvds

from ovds_utils.exceptions import VDSException
from ovds_utils.logging import get_logger
from ovds_utils.metadata import MetadataContainer
from ovds_utils.ovds import AccessModes, BrickSizes, Components, Dimensions, Formats, create_vds
from ovds_utils.ovds.utils import get_vds_info
from ovds_utils.ovds.writing import FORMAT2FLOAT

logger = get_logger(__name__)


class VDSChunk:
    def __init__(
        self,
        number: int,
        accessor: openvds.core.VolumeDataPageAccessor,
        page: openvds.core.VolumeDataPage,
        format: Formats
    ) -> None:
        super().__init__()
        self.is_released = False
        self.number = number
        self.accesor = accessor
        self.page = page
        self.format = format

    def __repr__(self) -> str:
        return f"<VDSChunk(number={self.number})>"

    def __getitem__(self, key: Sequence[Union[int, slice]]) -> np.array:
        dtype = FORMAT2FLOAT[self.format.value]
        buf = np.array(self.page.getWritableBuffer(), copy=False, dtype=dtype)
        return buf.__getitem__(key)

    def __setitem__(self, key: Sequence[Union[int, slice]], value: np.array):
        dtype = FORMAT2FLOAT[self.format.value]
        buf = np.array(self.page.getWritableBuffer(), copy=False, dtype=dtype)
        return buf.__setitem__(key, value)

    def release(self) -> None:
        self.page.release()
        self.is_released = True

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
        if self.n <= self.chunks_count:
            page = self.accessor.createPage(self.n)
            self.n += 1
            return VDSChunk(
                number=self.n, accessor=self.accessor, page=page, format=self.format
            )
        else:
            raise StopIteration


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
            begin: Sequence[int] = None,
            end: Sequence[int] = None
    ) -> None:
        self._vds_source = vds_source
        self.name = name
        self.format = format
        self.unit = unit
        self.accessor = accessor
        self.chunks_count = chunks_count
        self.begin = begin
        self.end = end
        self.shape = shape
        self.components = components
        self.value_range_min = value_range_min
        self.value_range_max = value_range_max

    def __repr__(self) -> str:
        return f"<Channel(name={self.name}, unit={self.unit}, format={self.format.name})>"

    def chunks(self) -> VDSChunksGenerator:
        return VDSChunksGenerator(chunks_count=self.chunks_count, accessor=self.accessor, format=self.format)

    def get_chunk(self, number: int) -> VDSChunk:
        if number not in set(range(self.chunks_count)):
            raise VDSException(f"Chunk number is out of range of: 0 to {self.chunks_count-1}")
        page = self.accessor.createPage(number)
        return VDSChunk(
            number=number, accessor=self.accessor, page=page, format=self.format
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

    def _getitem_for_subset_dataset(self, key: Sequence[Union[int, slice]]) -> np.array:
        # TODO: Add range checks
        new_key = []
        if all([isinstance(i, int) for i in key]):
            new_key = [k+self.begin[i] for i, k in enumerate(key)]
        elif all([isinstance(i, int) or isinstance(i, slice) for i in key]):
            for i, k in enumerate(key):
                if isinstance(k, slice):
                    new_key.append(
                        slice(
                            self.begin[i] + k.start if k.start else self.begin[i],
                            self.begin[i] + k.stop if k.stop else self.end[i],
                            None
                        )
                    )
                elif isinstance(k, int):
                    new_key.append(k+self.begin[i])
        else:
            raise VDSException("Item key is not list of slices or int")

        return self._getitem_for_whole_dataset(tuple(new_key))

    def __getitem__(self, key: Sequence[Union[int, slice]]) -> np.array:
        if self.begin and self.end:
            return self._getitem_for_subset_dataset(key)
        else:
            return self._getitem_for_whole_dataset(key)

    def commit(self):
        self.accessor.commit()


class VDS:
    def __init__(
        self,
        path: AnyStr,
        connection_string: AnyStr = "",
        shape: Sequence[int] = None,
        databrick_size: BrickSizes = BrickSizes.BrickSize_128,
        components: Components = Components.Components_1,
        metadata_dict: MetadataContainer = None,
        format: Formats = Formats.R32,
        data: np.array = None,
        begin: Sequence[int] = None,
        end: Sequence[int] = None,
        channels: List[Channel] = None,
    ) -> None:
        super().__init__()
        self.path = path
        self.connection_string = connection_string
        self.begin = begin
        self.end = end
        self._channels = {}
        if data is not None and shape is None:
            shape = data.shape
        try:
            self._vds_source = openvds.open(path, connection_string)
            vds_info = get_vds_info(path, connection_string)
        except RuntimeError as e:
            if str(e) in ("Open error: File::open "):
                if shape is None:
                    raise VDSException("Shape was not defined during creating new VDS source.")
                logger.debug("Creating new VDS source...")
                self.create(
                    path=path,
                    connection_string=connection_string,
                    shape=shape,
                    metadata_dict=metadata_dict,
                    databrick_size=databrick_size,
                    data=data,
                    channels=channels,
                    access_mode=AccessModes.Create,
                    components=components,
                    format=format,
                    begin=begin,
                    end=end
                )
                self = self.__init__(
                    path=path,
                    connection_string=connection_string,
                    shape=shape,
                    databrick_size=databrick_size,
                    components=components,
                    metadata_dict=metadata_dict,
                    format=format,
                    data=data,
                    begin=begin,
                    end=end
                )
                return
            else:
                raise VDSException(f"Open VDS resulted with: {str(e)}") from e
        self._layout = openvds.getLayout(self._vds_source)
        self._dimensionality = self._layout.getDimensionality()
        self._axis_descriptors = [
            self._layout.getAxisDescriptor(dim) for dim in range(self._dimensionality)
        ]
        self.chunks_count = self.count_number_of_chunks(self.shape, databrick_size)
        vds_info = get_vds_info(path, connection_string)
        _info = vds_info['layoutInfo'] if 'layoutInfo' in vds_info else vds_info
        for i, j in enumerate(_info['channelDescriptors']):
            self._channels[j['name']] = Channel(
                vds_source=self._vds_source,
                begin=self.begin,
                end=self.end,
                shape=self.shape,
                components=getattr(Components, j['components']),
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
        return list(self._channels.values())[number]

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

    def __str__(self) -> str:
        return f"<{self.__class__.__qualname__}(path={self.path})>"

    @property
    def axis_descriptors(self):
        return tuple(
            (a.name, a.unit, a.numSamples) for a in self._axis_descriptors[::-1]
        )

    @property
    def shape(self):
        if self.begin and self.end:
            return (
                self.end[2] - self.begin[2],
                self.end[1] - self.begin[1],
                self.end[0] - self.begin[0],
            )
        return tuple(int(a.numSamples) for a in self._axis_descriptors[::-1])

    @staticmethod
    def create(
        path: AnyStr,
        connection_string: AnyStr,
        shape: Sequence[int],
        databrick_size=BrickSizes.BrickSize_128,
        access_mode=AccessModes.Create,
        components=Components.Components_1,
        format=Formats.R32,
        metadata_dict: MetadataContainer = None,
        data: np.array = None,
        begin: Sequence[int] = None,
        end: Sequence[int] = None,
        channels: List[Channel] = None,
    ):
        return create_vds(
            path,
            connection_string,
            shape,
            databrick_size=databrick_size.value,
            access_mode=access_mode.value,
            metadata_dict=metadata_dict if metadata_dict else {},
            components=components.value,
            format=format.value,
            channels=channels,
            data=data,
            close=True,
            begin=begin,
            end=end
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
            dimensionsND=Dimensions.Dimensions_012,
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

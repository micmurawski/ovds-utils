from typing import Any, AnyStr, Dict, Union

import openvds

from ovds_utils.exceptions import VDSMetadataException
from ovds_utils.ovds import METADATATYPE2GETFUNCTION, METADATATYPE2SETFUNCTION, MetadataTypes


class MetadataValue:
    def __init__(self, value: Any, category: AnyStr, type: Union[AnyStr, MetadataTypes]) -> None:
        self.value = value
        self.category = category
        if isinstance(type, MetadataTypes):
            self._type = type
        elif str(type) not in METADATATYPE2GETFUNCTION:
            raise VDSMetadataException(
                f"The type {type} was not recognized among: {', '.join(METADATATYPE2GETFUNCTION.keys())}")
        else:
            self._type = getattr(MetadataTypes, str(type).replace("MetadataType.", ""))

    def __repr__(self) -> str:
        return f"<{self.__class__.__qualname__}(value={self.value}, category={self.category}, type={self.type})>"

    @property
    def type(self):
        return self._type.value


class MetadataContainer(dict):
    def __init__(self, **kwargs: Dict[AnyStr, MetadataValue]) -> None:
        super().__init__()
        self.update(kwargs)

    def __repr__(self) -> str:
        return f"<{self.__class__.__qualname__}({', '.join(self.keys())})>"

    def get_container(self):
        container = openvds.MetadataContainer()
        for k, v in self.items():
            set_method = METADATATYPE2SETFUNCTION[v.type]
            set_method(container, v.category, k, v.value)
        return container

    @staticmethod
    def get_from_layout(layout: openvds.core.VolumeDataLayout) -> openvds.core.MetadataContainer:
        metadata = {}
        for i in layout.getMetadataKeys():
            method = METADATATYPE2GETFUNCTION[str(i.type)]
            value = method(layout, i.category, i.name)
            metadata[i.name] = MetadataValue(value, i.category, i.type)

        return MetadataContainer(**metadata)

import warnings
from typing import Dict, List

import numpy as np
import openvds
from tqdm import tqdm

from ovds_utils.logging import get_logger

from .utils import create_metadata_container, read_metadata_container

logger = get_logger(__name__)


def _update_metadata_definition(
    existing_metadata,
    to_be_updated_metadata,
):
    """updates existing metadata with to be updated metadata entries"""
    updated_keys = [entry["name"] for entry in to_be_updated_metadata]
    existing_keys = [entry["name"] for entry in existing_metadata]
    for idx, key in enumerate(updated_keys):
        # replace whole entry
        if key in existing_keys:
            existing_idx = existing_keys.find(key)
            existing_metadata[existing_idx] = updated_keys[idx]
        else:
            existing_metadata.append(to_be_updated_metadata[idx])

    return existing_metadata


def copy_vds(
    source: str,
    target: str,
    source_connection: str = "",
    target_connection: str = "",
    updated_entries: List[Dict[str, str]] = None,
) -> str:
    """Python VDS API implementation of VDSCopy, that allows for customized behavior such as:
    * updating target metadata container
    * stripping LoD out of the source VDS

    """
    logger.info(
        "Starting copying the VDS. Source: %s Target: %s", str(source), str(target)
    )
    # NOTE: we support only one LOD at open-VDS level
    with openvds.open(source, source_connection) as source_handle:
        layout = openvds.getLayout(source_handle)
        source_layout_descriptor = layout.getLayoutDescriptor()
        if (
            source_layout_descriptor.getLODLevels()
            != openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_None
        ):
            warnings.warn(
                "Source VDS has more more LOD Levels, open-VDS supports only single LOD!"
                "Only single lod=0 will be copied!"
            )
        target_layout_descriptor = openvds.VolumeDataLayoutDescriptor(
            source_layout_descriptor.getBrickSize(),
            source_layout_descriptor.getNegativeMargin(),
            source_layout_descriptor.getPositiveMargin(),
            source_layout_descriptor.getBrickSizeMultiplier2D(),
            openvds.VolumeDataLayoutDescriptor.LODLevels.LODLevels_None,
            openvds.VolumeDataLayoutDescriptor.Options.Options_None,
            fullResolutionDimension=source_layout_descriptor.getFullResolutionDimension(),
        )
        axis = [
            layout.getAxisDescriptor(dim) for dim in range(layout.getDimensionality())
        ]
        channels = [
            layout.getChannelDescriptor(dim) for dim in range(layout.getChannelCount())
        ]
        # NOTE: fetch existing metadata container
        metadata_entries = read_metadata_container(layout)

        # update metadata entries with whatever is missing
        if updated_entries:
            metadata_entries = _update_metadata_definition(
                existing_metadata=metadata_entries,
                to_be_updated_metadata=updated_entries,
            )
        # create final ovds.MetadataContainer for target VDS
        target_metadata = create_metadata_container(metadata_entries)

        try:
            # create new with updated metadata
            with openvds.create(
                target,
                target_connection,
                target_layout_descriptor,
                axis,
                channels,
                target_metadata,
            ) as target_handle:
                manager_source = openvds.getAccessManager(source_handle)
                manager_target = openvds.getAccessManager(target_handle)

                # stolen from VDSCopy.cpp
                # NOTE: fix LOD range to one, since that's what working and supposedly only single LoD
                # is available in open-VDS
                lod_range = 1  # This ensures only LoD = 0 is copied
                MAX_PAGES = 8
                channel_count = layout.getChannelCount()
                logger.info(
                    f"Target VDS number of channels to be written {channel_count}"
                )
                # copy all levels of details
                for lod_idx in range(lod_range):
                    logger.debug(f"Target VDS writing LoD number: {lod_idx}")
                    source_accessors = [
                        manager_source.createVolumeDataPageAccessor(
                            dimensionsND=openvds.DimensionsND.Dimensions_012,
                            lod=lod_idx,
                            channel=channel_idx,
                            maxPages=MAX_PAGES,
                            accessMode=openvds.VolumeDataAccessManager.AccessMode.AccessMode_ReadOnly,
                        )
                        for channel_idx in range(channel_count)
                    ]
                    target_accessors = [
                        manager_target.createVolumeDataPageAccessor(
                            dimensionsND=openvds.DimensionsND.Dimensions_012,
                            lod=lod_idx,
                            channel=channel_idx,
                            maxPages=MAX_PAGES,
                            accessMode=openvds.VolumeDataAccessManager.AccessMode.AccessMode_Create,
                        )
                        for channel_idx in range(channel_count)
                    ]

                    # master "worker" loop
                    for channel_idx in range(channel_count):
                        for chunk_idx in tqdm(
                            range(source_accessors[channel_idx].getChunkCount()),
                            desc="Chunks copying",
                            unit="",
                        ):
                            mapped_chunk = source_accessors[
                                channel_idx
                            ].getMappedChunkIndex(chunk_idx)
                            source_page = source_accessors[channel_idx].readPage(
                                mapped_chunk
                            )
                            err = source_page.getError()
                            if err.errorCode:
                                logger.error(
                                    "Source VDS readPage failed: %s %s",
                                    str(err.errorCode),
                                    str(err.message),
                                )

                            target_page = target_accessors[channel_idx].createPage(
                                chunk_idx
                            )
                            err = target_page.getError()
                            if err.errorCode:
                                logger.error(
                                    "Target VDS createPage failed: %s %s",
                                    str(err.errorCode),
                                    str(err.message),
                                )

                            source_buffer = np.array(
                                source_page.getBuffer(), copy=False
                            )
                            target_buffer = np.array(
                                target_page.getWritableBuffer(), copy=False
                            )

                            target_buffer[:] = np.copy(source_buffer[:])

                            source_page.release()
                            target_page.release()

                        # commit changes to the target channel
                        target_accessors[channel_idx].commit()
        except RuntimeError as err:
            logger.error("Runtime Exception during openvds.create occurred!")
            logger.error("Source was: %s", str(source))
            logger.error("Target was: %s", str(target))
            logger.error("Do you have enough disk space for .vds allocation?")
            raise err

    return target

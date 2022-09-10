from subprocess import PIPE, Popen

from ovds_utils.logging import get_logger

from .utils import get_ovds_bin

logger = get_logger(__name__)


def copy_vds(
    source: str,
    target: str,
    source_connection: str = "",
    target_connection: str = "",
    additional_params={"tolerance": 1, "compression-method": None}
):
    cmd = [
        "OPENVDS_AWSCURL=1",
        get_ovds_bin("VDSCopy"),
        source,
        target,
        f"-s '{source_connection}'",
        f"-d '{target_connection}'",
    ]
    for k in additional_params:
        cmd.append(f"--{k} {additional_params[k]}")

    logger.info("Executing VDSCopy")
    process = Popen(
        " ".join(cmd), stdout=PIPE, stderr=PIPE, shell=True, universal_newlines=True
    )
    while process.poll() is None:
        info = process.stdout.readline()
        logger.info("VDSCopy:" + info)

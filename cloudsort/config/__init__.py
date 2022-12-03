import os
from typing import Optional

from cloudsort.config.aws import configs as aws_configs
from cloudsort.config.azure import configs as azure_configs
from cloudsort.config.common import (  # Expose these types to consumers of this module.
    CONFIG_NAME_ENV_VAR,
    AppConfig,
    Cloud,
    JobConfig,
    SystemConfig,
)
from cloudsort.config.local import configs as local_configs

__config_dict__ = {
    cfg.name: cfg for cfg in sum([aws_configs, azure_configs, local_configs], [])
}


def get(config_name: Optional[str] = None) -> JobConfig:
    if config_name is None:
        config_name = os.getenv(CONFIG_NAME_ENV_VAR)
    assert config_name, f"No configuration specified, please set ${CONFIG_NAME_ENV_VAR}"
    assert config_name in __config_dict__, f"Unknown configuration: {config_name}"
    return __config_dict__[config_name]

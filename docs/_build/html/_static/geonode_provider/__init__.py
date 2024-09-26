from typing import Any, Dict
import packaging.version


__version__ = "1.1.0"

try:
    from airflow import __version__ as airflow_version
except ImportError:
    from airflow.version import version as airflow_version

if packaging.version.parse(packaging.version.parse(airflow_version).base_version) < packaging.version.parse(
    "2.6.0"
):
    raise RuntimeError(
        f"The package `apache-airflow-providers-singularity:{__version__}` needs Apache Airflow 2.6.0+"
    )

def get_provider_info() -> Dict[str, Any]:
    return {
        "package-name": "airflow-providers-geonode",
        "name": "GeoNode Airflow Provider",
        "description": "A GeoNode provider for Apache Airflow.",
        "connection-types": [
            {"connection-type": "geonode", "hook-class-name": "geonode_provider.hooks.geonode.GeoNodeHook"}
        ],
        "versions": [__version__],
    }

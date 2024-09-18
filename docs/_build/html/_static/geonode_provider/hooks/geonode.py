# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# Provider developed by Fawzi El Gamal from KAN's Territory & IT ETL team.


from airflow.hooks.base import BaseHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException

from typing import Any, Dict
import requests
from requests.auth import HTTPBasicAuth
import re
import os
from io import BufferedReader, IOBase
import json

class GeoNodeHook(BaseHook):
    """
    Airflow hook to interact with GeoNode.
    
    Methods list:

    * import_layer: runs the "importlayers" command in the GeoNode container to upload the datasets contents in a specific directory/folder to GeoServer.
    * publish_layer: publishes a layer uploaded to the database in GeoServer.
    * update_layer: runs the "updatelayers" command in the GeoNode container so that the layer already uploaded, published and recognized by GeoServer, is now visible in GeoNode.
    * upload_dataset: uploads a layer in *.zip*, ".shp", "dbf", "shx", "prj" or ".tif" format to GeoNode via API v2.
    * upload_style: uploads a style in ".sld" format to GeoServer.
    * update_style: update an existing style.
    * update_layer_style: assigns an already uploaded style to a layer.
    * upload_map: creates a map in GeoNode via API v2 from a set of datasets.
    * upload_document: uploads a document to GeoNode via API v2.
    * upload_metadata: uploads/sets metadata for a given type of resource (for the moment title and supplementary information).
    * set_permiss: sets permissions for a given resource.
    * get_execution_information: gets information/metadata about a performed execution.
    """
    conn_name_attr = 'geonode_conn_id'
    default_conn_name = "geonode_default"
    conn_type = 'geonode'
    hook_name = 'GeoNode'

    def __init__(self, geonode_conn_id: str, source=None) -> None:
        super().__init__(source)
        self.conn = self.get_connection(geonode_conn_id)
        extra = self.conn.extra_dejson or {}
        self.base_url = self.conn.host
        self.gn_username = self.conn.login
        self.gn_password = self.conn.password
        self.gn_api_token = self._get_field(extra, "gn_api_token")
        self.gs_username = self._get_field(extra, "gs_username")
        self.gs_password = self._get_field(extra, "gs_password")
        self.ssh_host = self._get_field(extra, "ssh_host")
        self.ssh_port = self._get_field(extra, "ssh_port")
        self.ssh_username = self._get_field(extra, "ssh_username")
        self.ssh_password = self._get_field(extra, "ssh_password")

        if self.base_url == "":
            raise AirflowException('"Base URL", "GeoNode username", "GeoNode password" or "GeoNode Token" fields are required.')
        if not self.gn_api_token and not all([self.gn_username, self.gn_password]):
            raise AirflowException("You must to define an API Token or the GeoNode username and password.")
        if not all([self.gn_username, self.gn_password]) and not all([self.gs_username, self.gs_password]):
            raise AirflowException("You must to define GeoNode or GeoServer username and password to log into GeoServer.")
        
        if self.base_url.endswith("/"):
            self.base_url = self.base_url[:-1]

        if self.gs_username and not self.gs_password:
            raise AirflowException("If you define a GeoServer username you must also define the GeoServer password")
        elif self.gs_password and not self.gs_username:
            raise AirflowException("If you define a GeoServer password you must also define the GeoServer username")
        elif not self.gs_password and not self.gs_username:
            self.gs_password = self.gn_username
            self.gs_username = self.gn_password

        if not all([self.ssh_host, self.ssh_port, self.ssh_username, self.ssh_password]):
            raise AirflowException('You must to define SSH connection parameters.')

        # extras = self.conn.extra_dejson
        # self.token = extras["token"]

    def _get_field(self, extra_dict, field_name):
        prefix = "extra__geonode__"
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the '{prefix}' prefix "
                f"when using this method."
            )
        if field_name in extra_dict:
            return extra_dict[field_name] or None
        return extra_dict.get(f"{prefix}{field_name}") or None

    def test_connection(self):
        session = requests.Session()
        geonode_login = f"{self.base_url}/es-es/admin/login/"
        api_token_test_url = f"{self.base_url}/api/v2/users"
        geoserver_login = f"{self.base_url}/geoserver/j_spring_security_check"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": geonode_login
        }
        response = session.get(geonode_login, headers=headers)

        match = re.search(r'name="csrfmiddlewaretoken" value="([^"]+)"', response.text)
        
        if match:
            csrf_token = match.group(1)
            if len(csrf_token) > 60 and csrf_token.count(" ") == 0:
                csrf_token = csrf_token
            else:
                csrf_token = None

        if csrf_token:
            if self.gn_api_token:
                hearders = {"Authorization": f"Bearer {self.gn_api_token}"}
                response = requests.get(api_token_test_url, headers=hearders)
                if not response.status_code == 200:
                    self.log.error(f"Status code: {response.status_code}")
                    return False, "Error authenticating to GeoNode with GeoNode API Token"

            else:
                headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Referer": geonode_login
                }
                datos = {
                    "csrfmiddlewaretoken": csrf_token,
                    "username": self.gn_username,
                    "password": self.gn_password,
                    "next": "/es-es/admin/"
                }

                try:
                    response = session.post(geonode_login, headers=headers, data=datos)

                    if response.status_code != 200 and response.url != geonode_login[:-6]:
                        self.log.error(f"Status code: {response.status_code}")
                        return False, "Error authenticating to GeoNode with username and password"
                    
                except Exception as e:
                    return False, "Error authenticating to GeoNode", str(e) 

        else:
            return False, "Error getting to CSRF Token"

        # Authentication to GeoServer
        data = {
            "username": self.gs_username,
            "password": self.gs_password,
        }
        response = session.post(geoserver_login, data=data)

        if not response.url == f"{self.base_url}/geoserver/web/?0":
            self.log.error(f"Status code: {response.status_code}")
            self.log.error(f"Response url: {response.url}")
            return False, "Error authenticating to GeoServer"
        
        if all([self.ssh_host, self.ssh_port, self.ssh_username, self.ssh_password]):
            ssh = SSHHook(
                    remote_host=self.ssh_host,
                    port=self.ssh_port,
                    username=self.ssh_username, 
                    password=self.ssh_password
                )
            ssh_test = ssh.test_connection()
            if ssh_test[0]:
                return True, "Connection successfully tested"
            else:
                return False, f"SSH {ssh_test[1]}"

    def import_layer(self, layers_folder_path:str) -> None:
        """
        This method allows to update datasets to GeoServer by passing datasets directory. Just works with directories, not with single files.
        The method uses an SSH connection to execute a Docker command within GeoNode's container.

        :param layers_folder_path: Path to datasets directory/folder.
        """
        ssh = SSHHook(
            remote_host=self.ssh_host,
            port=self.ssh_port,
            username=self.ssh_username, 
            password=self.ssh_password
        )

        command = f'docker exec django4geonode python manage.py importlayers {layers_folder_path}'

        try:
            with ssh.get_conn() as ssh_client:
                exit_status, agg_stdout, agg_stderr = ssh.exec_ssh_client_command(
                ssh_client, command, timeout=1800, environment=None, get_pty=True
            )

            agg_stdout = agg_stdout.decode('utf-8')
            pattern = re.compile(r"Output data: \{'success': \[(.*?)\], 'errors': \[(.*?)\]\}")
            # pattern = re.compile(rf"Output data: \{{'success': \['{re.escape(os.path.basename(layers_folder_path))}'\], 'errors': \[\]\}}")
            match = pattern.search(agg_stdout)
            if match:
                success_files = match.group(1)
                error_files = match.group(2)
                if not success_files.strip() or error_files:
                    raise AirflowException(f"Error importing layers from {layers_folder_path} path. Error files {error_files}")                    
            else:
                raise AirflowException(f"Error importing layers from {layers_folder_path} path.")

            if exit_status != 0:
                raise AirflowException(f"Error importing layers from {layers_folder_path} path")
            elif agg_stdout:
                self.log.info(f"exit_status {exit_status}")
                self.log.info(f"agg_stderr {agg_stderr}")
                self.log.info('Layers imported successfully')
                return
            else:
                self.log.warning(f"exit_status {exit_status}")
                self.log.warning(f"agg_stderr {agg_stderr}")
                self.log.error(f'Error importing layers: {str(e)}')
                raise AirflowException('Layer import failed')
        except Exception as e:
            self.log.error(f'Error importing layers: {str(e)}')
            raise AirflowException('Layer import failed')

    def publish_layer(self, layer_names) -> None:
        """
        This method is utilized to publish a layer to GeoServer by utilizing an API 'POST' request.

        :param layer_names: The dataset name (not title) that must be published. Is possible to pass a list of datasets names to execute this method recursively.
        """
        login = HTTPBasicAuth(self.gs_username, self.gs_password)
        headers = {'Content-Type': 'application/xml'}
        url = f'{self.base_url}/geoserver/rest/workspaces/geonode/datastores/geonode_data/featuretypes'

        def publish(layer_name):
            xml = f'<featureType><name>{layer_name}</name><enabled>false</enabled></featureType>'
            
            try:
                r = requests.post(url=url, auth=login, data=xml, headers=headers, verify=False)
                if r.status_code == 201:
                    self.log.info(f'Layer {layer_name} published successfully.')
                elif r.status_code == 500 and f"Resource named '{layer_name}' already exists in store" in r.text:
                    self.log.warning(f"Resource named '{layer_name}' already exists in store")
                else:
                    self.log.error(f'Error publishing layer {layer_name}. Status code: {r.status_code}. Response {r.text}')
                    raise AirflowException()
            except Exception as e:
                raise AirflowException(str(e))
        
        if isinstance(layer_names, list):
            for layer_name in layer_names:
                publish(layer_name)
        elif isinstance(layer_names, str):
            publish(layer_names)

        return

    def update_layer(self, layer_names:str):
        """
        The update_layer method is an operator responsible for executing the update of layers.
        The method uses an SSH connection to execute a Docker command within GeoNode's container.

        :param layer_name: The dataset name (not title) that must be updated. Is possible to pass a list of datasets names to execute this method recursively.

        Raises:
            AirflowException: Raised if an error occurs during the layer update process.
        """
        self.log.info('Executing update_layer operator')

        ssh = SSHHook(
            remote_host=self.ssh_host,
            port=self.ssh_port,
            username=self.ssh_username, 
            password=self.ssh_password
        )

        def update(layer_name):
            command = f'docker exec django4geonode python manage.py updatelayers -f {layer_name}'
            try:
                with ssh.get_conn() as ssh_client:
                    exit_status, agg_stdout, agg_stderr = ssh.exec_ssh_client_command(
                    ssh_client, command, timeout=60, environment=None, get_pty=True
                )

                if exit_status != 0:
                    raise AirflowException(f"Error updating layers {layer_name}")
                elif agg_stdout:
                    self.log.info(f"exit_status {exit_status}")
                    self.log.info(f"agg_stderr {agg_stderr}")
                    self.log.info('Layer update successful')
                else:
                    self.log.warning(f"exit_status {exit_status}")
                    self.log.warning(f"agg_stderr {agg_stderr}")
                    self.log.error(f'Error importing layers: {str(e)}')
                    raise AirflowException('Layer update failed')
            except Exception as e:
                self.log.error(f'Error updating layer: {str(e)}')
                raise AirflowException('Layer update failed')

        if isinstance(layer_names, list):
            for layer_name in layer_names:
                update(layer_name)
        elif isinstance(layer_names, str):
            update(layer_names)
        
        return

    def upload_dataset(self, dataset_path:str) -> str:
        """
        This method upload a dataset by GeoNode v2 API.
        Datasets can be in the following formats:
        - .zip
        - .shx, .prj, .shp and .dbf
        - .tif

        :param dataset_path: The dataset path that must be uploaded. The path can be a directory/folder or a single file.

        Returns: 
            For vector .zip datasets, a dictionary (json) is returned with the ID of the dataset. For individual vector or rasters datasets, the run/execution ID is returned.
        """
        if os.path.exists(dataset_path):
            base, ext = os.path.splitext(dataset_path)
            dataset_title = os.path.basename(dataset_path)[:-4]
            params = {
                "time": "false",
                "dataset_title": dataset_title,
                "charset": "UTF-8",
            }
            if self.gn_api_token:
                headers = {"Authorization": f"Bearer {self.gn_api_token}"}
            else:
                auth = HTTPBasicAuth(self.gn_username, self.gn_password)
            client = requests.session()
            if not ext: # For directories/folders
                for root, subdirs, files in os.walk(dataset_path):
                    resources_uploaded = dict()
                    for file in files:
                        _file = os.path.join(root, file)
                        print(f"Scanning: {_file}")
                        if not os.path.exists(_file):
                            print(f"The selected file path does not exist: {_file}")
                            continue
                        spatial_files = ("dbf_file", "shx_file", "prj_file", "xml_file", "sld_file")
                        base, ext = os.path.splitext(_file)
                        params = {
                            "time": "false",
                            "dataset_title": file,
                            "charset": "UTF-8",
                        }
                        if ext.lower() == ".shp":
                            if os.path.exists(f'{base}.sld'):
                                params["sld_file"] = open(f'{base}.sld', "rb")
                            for spatial_file in spatial_files:
                                ext, _ = spatial_file.split("_")
                                file_path = f"{base}.{ext}"
                                # sometimes a shapefile is missing an extra file,
                                # allow for that
                                if os.path.exists(file_path):
                                    params[spatial_file] = open(file_path, "rb")
                        elif ext.lower() in (".tif", ".zip"):
                            file_path = base + ext
                            params["tif_file"] = open(file_path, "rb")
                            if os.path.exists(f'{base}.xml'):
                                params["xml_file"] = open(f'{base}.xml', "rb")
                            if os.path.exists(f'{base}.sld'):
                                params["sld_file"] = open(f'{base}.sld', "rb")

                        else:
                            continue

                        files = {}

                        with open(_file, "rb") as base_file:
                            params["base_file"] = base_file
                            for name, value in params.items():
                                if isinstance(value, BufferedReader):
                                    files[name] = (os.path.basename(value.name), value)
                                    params[name] = os.path.basename(value.name)

                            params["non_interactive"] = 'true'
                            try:
                                if self.gn_api_token:
                                    response = client.post(
                                        url = f'{self.base_url}/api/v2/uploads/upload/',
                                        headers=headers,
                                        data = params,
                                        files = files,
                                    )
                                else:
                                    response = client.post(
                                        url = f'{self.base_url}/api/v2/uploads/upload/',
                                        auth=auth,
                                        data = params,
                                        files = files,
                                    )
                                if response.status_code in (200, 201):
                                    if isinstance(params.get("tif_file"), IOBase):
                                        params["tif_file"].close()
                                        params["sld_file"].close()

                                    response = response.json()
                                    if ext == ".zip":
                                        resource_url_splited = response["url"].split("/")
                                        dataset_id = resource_url_splited[len(resource_url_splited)-1]
                                        self.log.info(f"Dataset name: {params[name]}. Dataset ID: {dataset_id}")
                                        resources_uploaded[params[name]] = dataset_id
                                    else:
                                        self.log.info(f"Dataset uploaded successfully. {response}")
                                        self.log.info(f"Dataset name: {params[name]}. Execution ID: {response['execution_id']}")
                                        resources_uploaded[params[name]] = response["execution_id"]
                                else:
                                    self.log.error(f"An error occurred uploading dataset {response.status_code} {response.text}")
                                    raise AirflowException()
                            except Exception as e:
                                raise AirflowException(str(e))

                    self.log.info(f"Resources uploaded successfully from {dataset_path}. {resources_uploaded}")
                    return resources_uploaded

            elif ext in (".zip", ".tif", ".shp"): # For files
                self.log.info(f"Dataset path: {dataset_path}")

                spatial_files = ("dbf_file", "shx_file", "prj_file", "xml_file", "sld_file")
                base, ext = os.path.splitext(dataset_path)
                params = {
                    "time": "false",
                    "charset": "UTF-8",
                }

                if ext.lower() == ".shp":
                    if os.path.exists(f'{base}.sld'):
                        params["sld_file"] = open(f'{base}.sld', "rb")
                    for spatial_file in spatial_files:
                        ext, _ = spatial_file.split("_")
                        file_path = f"{base}.{ext}"
                        # sometimes a shapefile is missing an extra file,
                        # allow for that
                        if os.path.exists(file_path):
                            params[spatial_file] = open(file_path, "rb")
                else:
                    params["tif_file"] = open(dataset_path, "rb")
                    if os.path.exists(f'{base}.xml'):
                        params["xml_file"] = open(f'{base}.xml', "rb")
                    if os.path.exists(f'{base}.sld'):
                        params["sld_file"] = open(f'{base}.sld', "rb")

                files = {}

                with open(dataset_path, "rb") as base_file:
                    params["base_file"] = base_file
                    for name, value in params.items():
                        if isinstance(value, BufferedReader):
                            files[name] = (os.path.basename(value.name), value)
                            params[name] = os.path.basename(value.name)

                    params["non_interactive"] = 'true'

                    try:
                        if self.gn_api_token:
                            response = client.post(
                                url = f'{self.base_url}/api/v2/uploads/upload/',
                                headers=headers,
                                data = params,
                                files = files,
                            )
                        else:
                            response = client.post(
                                url = f'{self.base_url}/api/v2/uploads/upload/',
                                auth=auth,
                                data = params,
                                files = files,
                            )
                        if response.status_code in (200, 201):
                            if isinstance(params.get("tif_file"), IOBase):
                                params["tif_file"].close()
                                params["sld_file"].close()

                            response = response.json()
                            if ext == ".zip":
                                resource_url_splited = response["url"].split("/")
                                dataset_id = resource_url_splited[len(resource_url_splited)-1]
                                self.log.info(f"Dataset uploaded successfully. {response}")
                                return {"dataset_id": dataset_id}
                            else:
                                self.log.info(f"Dataset name: {params[name]}. Execution ID: {response['execution_id']}")
                                return {"execution_id": response["execution_id"]}
                        else:
                            self.log.error(f"An error occurred uploading dataset {response.status_code} {response.text}")
                            raise AirflowException()
                    except Exception as e:
                        raise AirflowException(str(e))
            else:
                self.log.error(f'Dataset format are not supported. Extension must to be ".tif", ".zip" or ".shp".')
                raise AirflowException()
        else:
            self.log.error(f'The path provided not exist.')
            raise AirflowException()

    def upload_style(self, style_path:str, style_name:str=None) -> None:
        """
        Allows to upload a style in '.sld' format.

        :param style_path: Path to ".sld" style file.
        :param style_name: Optional. Name that the style will have in GeoServer. If isn't defined it will take the name of ".sld" file.
        """
        if not style_path.endswith(".sld"):
            self.log.error(f'Style file must to be in ".sld" format')
            raise AirflowException()

        if not style_name:
            style_name = os.path.basename(style_path)[:-4]

        url = f"{self.base_url}/geoserver/rest/workspaces/geonode/styles?name={style_name}"

        headers = {
            'Content-Type': 'application/vnd.ogc.sld+xml',
            }

        with open(style_path, 'r') as f:
            style = f.read()
        
        try:
            response = requests.post(url=url, data=style, auth=(self.gs_username, self.gs_password), headers=headers, verify=False)
            if response.status_code == 201:
                self.log.info(f'Style "{style_name}" uploaded successfully.')
                return
            else:
                if f'Style {style_name} already exists' in response.text:
                    self.log.error(f"Style {style_name} already exists")
                else:
                    self.log.error(f"Error uploading style {style_name}. Status code: {response.status_code}. {response.text}")
                raise AirflowException()
        except Exception as e:
            raise AirflowException(str(e))

    def update_style(self, style_path:str, style_name:str=None) -> None:
        """
        Allows to update an existing style in '.sld' format.

        :param style_path: Path to ".sld" style file.
        :param style_name: Optional. Name that the style will have in GeoServer. If isn't defined it will take the name of ".sld" file.
        """
        if not style_path.endswith(".sld"):
            self.log.error(f'Style file must to be in ".sld" format')
            raise AirflowException()

        if not style_name:
            style_name = os.path.basename(style_path)[:-4]

        url = f"{self.base_url}/geoserver/rest/workspaces/geonode/styles/{style_name}"

        headers = {
            'Content-Type': 'application/vnd.ogc.sld+xml',
            }

        with open(style_path, 'r') as f:
            style = f.read()
        
        try:
            response = requests.put(url=url, data=style, auth=(self.gs_username, self.gs_password), headers=headers, verify=False)
            if response.status_code == 200:
                self.log.info(f'Style "{style_name}" updated successfully.')
                return
            else:
                if f'Invalid style:null' in response.text:
                    self.log.error(f"The style {style_name} does not exist or the new style is invalid.")
                else:
                    self.log.error(f"Error updating style {style_name}. Status code: {response.status_code}. {response.text}")
                raise AirflowException()
        except Exception as e:
            raise AirflowException(str(e))

    def update_layer_style(self, layer_name:str, style_name:str) -> None:
        """
        This method is employed to update the styles of a layer by utilizing an GeoServer API 'PUT' request.

        :param layer_name: The layer/dataset name (not title) which want to apply the style.
        :param style_name: The style name (already uploaded) wich want to apply to the layer/dataset.
        """
        url = f'{self.base_url}/geoserver/rest/layers/geonode:{layer_name}'
        auth = (self.gs_username, self.gs_password)
        headers = {'Content-type': 'text/xml'}
        data = f'<layer><defaultStyle><name>{style_name}</name></defaultStyle></layer>'

        try:
            response = requests.put(url, auth=auth, headers=headers, data=data)
            if response.status_code == 200:
                self.log.info(f'Style "{style_name}" updated successfully for {layer_name} dataset.')
                return
            else:
                self.log.error(f'Failed to update {layer_name} style. Status code: {response.status_code}')
                raise AirflowException()
        except Exception as e:
            raise AirflowException(str(e))

    def upload_map(self, datasets_ids:list, map_title:str) -> str:
        """
        The upload_map method is responsible for creating a map in GeoNode via API v2.
        You must to choice datasets with the same projection and put them into a list. First put uppers datasets and then lowers (o base) datasets.

        :param datasets_ids: List of datasets ids that should make up the map. Sorted from the outermost/upper layer to the lower ones.
        :param map_title: The title to be placed on the map.

        Returns:
            This method returns a map ID just created.
        """
        srid = None
        maplayers_metadata = list()
        layers_metadata = list()
        bbox_list = list()
        for dataset_id in datasets_ids:
            map_create_url = f"{self.base_url}/api/v2/datasets/{dataset_id}/"
            self.log.info(f"Requesting: {map_create_url}")
            try:
                if self.gn_api_token:
                    headers = {"Authorization": f"Bearer {self.gn_api_token}"}
                    response = requests.get(map_create_url, headers=headers)
                else:
                    response = requests.get(map_create_url, auth=(self.gn_username, self.gn_password))

                if response.status_code in [200, 201]:
                    data = response.json()
                    if not data["dataset"]:
                        self.log.error("There is no dataset with the entered ID")
                        raise AirflowException("There is no dataset with the entered ID")                        
                else:
                    self.log.error(f"Error: {response.status_code} - {response.text}")
                    raise AirflowException(f"Error getting dataset information")
            except Exception as e:
                raise AirflowException(str(e))
            
            if not srid:
                srid = data["dataset"]["srid"]
            elif data["dataset"]["srid"] != srid:
                self.log.error(f"Datasets projections must be are the same")
                raise AirflowException()
            
            maplayer_metadata = {"extra_params": 
                {"msId": data['dataset']['uuid']},
                "current_style": data['dataset']['alternate'],
                "name": data['dataset']['alternate'],
            }
            layer_metadata =  {
                        "id": data['dataset']['uuid'],
                        "name": data['dataset']['title'],
                        "type": "",
                        "group": "background",
                        "title": data['dataset']['title'],
                        "hidden": False,
                        "source": "",
                        "dimensions": [],
                        "singleTile": False,
                        "visibility": False,
                        "hideLoading": False,
                        "useForElevation": False,
                        "handleClickOnLayer": False,
                        "provider": "",
                        "thumbURL": data['dataset']['thumbnail_url'],
                        "styles": data['dataset']['alternate'],
                        "extraParams": {"msId": data['dataset']['uuid']},
                    }
            maplayers_metadata.append(maplayer_metadata)
            layers_metadata.append(layer_metadata)
            bbox_list.append(data["dataset"]["bbox_polygon"]["coordinates"])

        # Lists to store minimum and maximum x and y coordinates
        x_min_list = []
        y_min_list = []
        x_max_list = []
        y_max_list = []

        # Iterates over each set of bbox coordinates
        for bbox in bbox_list:
            # Extrae las coordenadas x e y de cada bbox
            x_coords = [coord[0] for coord in bbox[0]]
            y_coords = [coord[1] for coord in bbox[0]]
            
            # Calculates the minimum and maximum x and y coordinates of each bbox
            x_min_list.append(min(x_coords))
            y_min_list.append(min(y_coords))
            x_max_list.append(max(x_coords))
            y_max_list.append(max(y_coords))

        # Calculates the global minimum and maximum x and y coordinates
        x_min_global = min(x_min_list)
        y_min_global = min(y_min_list)
        x_max_global = max(x_max_list)
        y_max_global = max(y_max_list)

        # Calculates the maximum extension
        max_extension = {
            'minx': x_min_global,
            'miny': y_min_global,
            'maxx': x_max_global,
            'maxy': y_max_global
        }

        # Calculates the centroid
        centroide_x = (x_min_global + x_max_global) / 2
        centroide_y = (y_min_global + y_max_global) / 2

        print(f"maxExtent: {list(max_extension.values())}")

        mapdata = {
            "map": {
                "zoom": 3,
                "units": "m",
                "center": {"x": centroide_x, "y": centroide_y, "crs": srid},
                "groups": [{"id": "Default", "title": "Default", "expanded": True}],
                "maxExtent": list(max_extension.values()),
                "mapOptions": {},
                "projection": srid,
                "backgrounds": [],
            },
            "version": 2,
            "timelineData": {},
            "dimensionData": {},
            "widgetsConfig": {"layouts": {"md": [], "xxs": []}},
            "catalogServices": {
                "services": {
                    "GeoNode Catalogue": {
                        "url": f"{self.base_url}/catalogue/csw",
                        "type": "csw",
                        "title": "GeoNode Catalogue",
                        "autoload": True,
                    },
                },
                "selectedService": "GeoNode Catalogue",
            },
            "mapInfoConfiguration": {},
        }

        mapdata['map']['layers'] = layers_metadata

        map_data = {
            "title": map_title,
            "data": mapdata,
            "maplayers": maplayers_metadata,
        }

        url = f"{self.base_url}/api/v2/maps/"
        try:
            if self.gn_api_token:
                headers = {"Authorization": f"Bearer {self.gn_api_token}"}
                response = requests.post(url, json=map_data, headers=headers)
            else:
                response = requests.post(url, json=map_data, auth=(self.gn_username, self.gn_password))

            if response.status_code == 201:
                map_id = response.json()['map']["pk"]
                self.log.info(f'Map created successfully with ID: {map_id}')
                return map_id
            else:
                self.log.error(f"An error occurred creating the map {response.status_code} {response.text}")
                raise AirflowException()
        except Exception as e:
            raise AirflowException(str(e))

    def upload_document(self, document_path:str) -> str:
        """
        This method uploads a document to GeoNode via API v2.

        :param document_path: Path to document that want to be uploaded.

        Returns:
            This method returns the document ID that just uploaded.
        """
        url = f'{self.base_url}/api/v2/documents'
        document_title = os.path.basename(document_path)

        payload = {'title': document_title}

        files = [('doc_file', (document_path, open(document_path, 'rb'), 'document/pdf'))]

        try:
            if self.gn_api_token:
                headers = {"Authorization": f"Bearer {self.gn_api_token}"}
                response = requests.post(url, data=payload, headers=headers, files=files)
            else:
                response = requests.post(url, data=payload, auth=(self.gn_username, self.gn_password), files=files)

            if response.status_code == 201:
                self.log.info(f'Document uploaded successfuly. Status code: {response.status_code}. {response.text}')
                document_id = response.json()["document"]["pk"]
                return document_id
            else:
                self.log.error(f"Failed uploaded document: {response.status_code}. {response.text}")
                raise AirflowException()
        except Exception as e:
            raise AirflowException(str(e))
        
    def upload_metadata(self, resource_type:str, resource_id:int, resource_title:str=None, resource_supplemental_information:str=None) -> None:
        """
        The upload_metadata method is designed to update metadata for any resource type on GeoNode by API v2
        Metadata allowed:

        - Title
        - Supplemental information

        :param resource_type: Type of resource wich want to be updated his metadata.
            Resource type allowed:

            - dataset
            - documents
            - maps

        :param resource_id: ID of the resource whose metadata want to update.
        :param resource_title: The title to be placed on the resource.
        :param resource_supplemental_information: The supplemental information to be placed on the resource.
        """
        if not resource_type.endswith("s"):
            resource_type = f"{resource_type}s"

        if not resource_type in ["datasets", "documents", "maps"]:
            self.log.error(f'Resource type not allowed')
            raise AirflowException()
        
        url = f'{self.base_url}/api/v2/{resource_type}/{resource_id}'
        headers = {
            'Content-Type': 'application/json',
            }

        metadata_data = dict()
        if resource_title:
            metadata_data["title"] = resource_title
        if resource_supplemental_information:
            metadata_data["supplemental_information"] = resource_supplemental_information

        try:
            if self.gn_api_token:
                headers["Authorization"] = f"Bearer {self.gn_api_token}"
                response = requests.patch(url, json=metadata_data, headers=headers)
            else:
                response = requests.patch(url, json=metadata_data, headers=headers, auth=(self.gn_username, self.gn_password))

            if response.status_code == 200:
                self.log.info('Metadata patched successfully!')
                self.log.info(response.json())
                return
            else:
                self.log.error(f'Error when performing PATH: {response.status_code} - {response.text}')
                raise AirflowException()
        except Exception as e:
            raise AirflowException(str(e))

    def set_permiss(self, resource_id:int, permiss_data:dict):
        """
        The `set_permiss` method is responsible for setting permissions for a specified resource.
        It targets the permissions API v2 endpoint and uses a 'PATCH' request to update the permissions based on the provided `permiss_data`.

        :param resource_id: ID of the resource whose permiss want to update.
        :param permiss_data: JSON/dict object with desired permission settings. 
        
        Example JSON:
        .. code-block:: json

            {
                "users": [
                {
                "id": <user_id>,
                "permissions": "none"
                },
                {
                "id": <user_id>,
                "permissions": "view"
                },
                {
                "id": <user_id>,
                "permissions": "download"
                },
                {
                "id": <user_id>,
                "permissions": "edit"
                },
                {
                "id": <user_id>,
                "permissions": "manage"
                },
                {
                "id": <user_id>,
                "permissions": "owner"
                }
            ],
                "groups": [
                {
                "id": <group_id>,
                "permissions": "none"
                },
                {
                "id": <group_id>,
                "permissions": "none"
                }
            ],
                "organizations": [
                {
                "id": <organization_id>,
                "permissions": "edit"
                }

            ]

        }
        """
        try:
            url = f'{self.base_url}/api/v2/resources/{resource_id}/permissions'

            if "groups" not in permiss_data:
                permiss_data["groups"] = []
            if "organizations" not in permiss_data:
                permiss_data["organizations"] = []

            if self.gn_api_token:
                headers = {"Authorization": f"Bearer {self.gn_api_token}"}
                response = requests.put(url, headers=headers, json=permiss_data)
            else:
                response = requests.put(url, auth=(self.gn_username, self.gn_password), json=permiss_data)

            if response.status_code == 200:
                response_dict = json.loads(response.text)
                self.log.info(f"Execution ID: {response_dict['execution_id']}")
                self.log.info(f"Execution details: {response_dict['status_url']}")
            else:
                self.log.error({response.text})
                raise AirflowException("An error occurred setting permissions.")
            
            return response.text

        except Exception as e:
            raise AirflowException(str(e))
    
    def _get_execution_information(self, execution_id:str) -> dict:
        """
        Method that gets metadata/information from a given execution. For example when a resource is uploaded.

        :param execution_id: ID of the execution from which want to obtain the information.
        """
        url = f"{self.base_url}/api/v2/resource-service/execution-status/{execution_id}"
        headers = {
            'Content-Type': 'application/json',
            }
        self.log.info(f"Execution ID: {execution_id}")

        try:
            if self.gn_api_token:
                headers["Authorization"] = f"Bearer {self.gn_api_token}"
                response = requests.get(url, headers=headers)
            else:
                response = requests.get(url, headers=headers, auth=(self.gn_username, self.gn_password))

            if response.status_code == 200:
                self.log.info(f"Execution information: {response.json()}")
                return response.json()
            else:
                self.log.error(f'Error getting execution information: {response.status_code} - {response.text}')
                raise AirflowException()
        except Exception as e:
            raise AirflowException(str(e))

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget, BS3PasswordFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "gn_api_token": StringField(
                lazy_gettext("GeoNode API Token"), 
                widget=BS3PasswordFieldWidget(),
                description="GeoNode API Token. Optional, you can log-in with normal credentials (username, password) or with a token. You must provide one of the two"
            ),
            "gs_username": StringField(
                lazy_gettext("GeoServer username"), 
                widget=BS3TextFieldWidget(),
                description="GeoServer username. Optional, if GeoServer credentials are different from GeoNode credentials."
            ),
            "gs_password": StringField(
                lazy_gettext("GeoServer password"), 
                widget=BS3PasswordFieldWidget(),
                description="GeoServer password. Optional, if GeoServer credentials are different from GeoNode credentials."
            ),
            "ssh_host": StringField(
                lazy_gettext("SSH host"), 
                widget=BS3TextFieldWidget(),
                description="SHH host to GeoNode server"
            ),
            "ssh_port": StringField(
                lazy_gettext("SSH port"), 
                widget=BS3TextFieldWidget(),
                description="SHH port to GeoNode server"
            ),
            "ssh_username": StringField(
                lazy_gettext("SSH username"), 
                widget=BS3TextFieldWidget(),
                description="SHH username to GeoNode server"
            ),
            "ssh_password": StringField(
                lazy_gettext("SSH password"), 
                widget=BS3PasswordFieldWidget(),
                description="SHH password to GeoNode server"
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        return {
            "hidden_fields": ["port", "schema", "extra"],
            "relabeling": {
                "host": "Base URL",
                "login": "GeoNode username",
                "password": "GeoNode password",
            },
            "placeholders": {
                "host": 'GeoNode base url. For example: "https://my-geonode-url.com"',
                "login": "GeoNode username",
                "password": "GeoNode password",
                "gs_username": "GeoServer username (optional, if GeoServer credentials are different from GeoNode credentials).",
                "gs_password": "GeoServer password (optional, if GeoServer credentials are different from GeoNode credentials).",
                "gn_api_token": "API Token (optional, you can log-in with normal credentials (username, password) or with a token. You must provide one of the two)",
                "ssh_host": "SHH host to GeoNode server",
                "ssh_port": "SHH port to GeoNode server",
                "ssh_username": "SHH username to GeoNode server",
                "ssh_password": "SHH password to GeoNode server",
                },
        }

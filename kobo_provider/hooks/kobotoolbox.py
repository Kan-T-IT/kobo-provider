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
from airflow.exceptions import AirflowException

from typing import Dict
import requests


class KoboHook(BaseHook):
    """
    Interact with KoboToolBox.
    Methods list:

        * get_form_ids: Allows to obtain the formid and the id_string of a form providing his formid or his id_string.
        * get_form_metadata: Gets the metadata of a form.
        * get_form_columns: Gets form columns.
        * get_form_submission: Gets the submissions or responses for a form.
    """
    conn_name_attr = 'kobo_conn_id'
    default_conn_name = "kobo_default"
    conn_type = 'kobotoolbox'
    hook_name = 'KoboToolBox'

    def __init__(self, kobo_conn_id:str, source=None) -> None:
        super().__init__(source)
        self.conn = self.get_connection(kobo_conn_id)
        self.base_url = self.conn.host
        self.kc_base_url = self.conn.login
        self.token = self.conn.password

        if not all([self.base_url, self.kc_base_url, self.token]):
            raise AirflowException('"Kobo Host", "Kobo CAT Host" and "API Token" are required.')
        
        if self.base_url.endswith("/"):
            self.base_url = self.base_url[:-1]
        if self.kc_base_url.endswith("/"):
            self.kc_base_url = self.kc_base_url[:-1]

    def test_connection(self):
        url = f"{self.kc_base_url}/api/v1/metadata"
        headers = {
            "Authorization": f"Token {self.token}"
        }
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return True, "Connection successfully tested"
            else:
                return False, "Connection failed. Check provided credentials"
        except Exception as e:
            return False, str(e)

    def get_form_ids(self, formid:int = None, id_string:str = None) -> int:
        """
        This method allows to get a formid and id_string for a form, providing his formid or his id_string.

        :param formid: formid of a form.
        :type formid: int
        :param id_string: id_string of a form.
        :type id_string: str

        Returns:
            This method returns the formid and the id_string for a form.
        """
        headers = {
            "Authorization": f"Token {self.token}"
            }
        try:
            response = requests.get(f"{self.kc_base_url}/api/v1/data", headers=headers)
            if response.status_code == 200:
                for form in response.json():
                    if (formid and form["id"] == formid) or (id_string and form["id_string"] == id_string):
                        self.log.info(f"Formid: {form['id']}, id_string: {form['id_string']}")
                        return form["id"], form["id_string"]

                if formid:
                    self.log.error(f'There is no form with the provided ID: {formid}.')
                elif id_string:
                    self.log.error(f'There is no form with the provided ID string: {id_string}.')
                raise Exception("Failed to retrieve formid and id_string.")
            else:
                self.log.error(f"Failed to retrieve formid and id_string. Status code: {response.status_code} {response.text}")
                raise Exception("Failed to retrieve formid and id_string.")
        except Exception as e:
            raise AirflowException(str(e))

    def get_form_metadata(self, formid:int) -> dict:
        """
        Retrieve metadata of a specific form.

        :param formid: Form ID to be fetched
        :type formid: int

        Returns:
            A JSON object (or python dict) with the form metadata.
        """
        self.log.info(f"Form ID: {formid}")
        headers = {
            "Authorization": f"Token {self.token}"
            }
        try:
            response = requests.get(f"{self.kc_base_url}/api/v1/forms/{formid}", headers=headers)
            if response.status_code == 200:
                self.log.info("Form metadata retrieved successfully.")
                return response.json()
            else:
                self.log.error(f"Failed to retrieve form metadata. Status code: {response.status_code} {response.text}")
                raise Exception("Failed to retrieve form metadata.")
        except Exception as e:
            raise AirflowException(str(e))

    def get_form_columns(self, id_string:str) -> dict:
        """
        This method gets all columns for a specific form

        :param id_string: id_string of a form.
        :type id_string: str

        Returns:
            A JSON object (or python dict) with the form columns and their types.
        """
        url = f"{self.base_url}/api/v2/assets/{id_string}/?format=json"
        headers = {
        "Authorization": f"Token {self.token}",
        "Content-Type": "application/json"
        }
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                allowed_types = (
                            "text",
                            "integer",
                            "decimal",
                            "range",
                            "select_one",
                            "select_multiple",
                            "date",
                            "time",
                            "datetime",
                            "image",
                            "audio",
                            "video",
                            "file",
                            "geopoint", 
                            "geotrace", 
                            "geoshape",
                            "calculate",
                            "barcode",
                            "acknowledge",
                            "score__row",
                            "rank__level"
                        )
                columns = dict()
                geometric_columns = []
                repeat_columns = []
                self.log.info("All form columns (without form metadata columns):")
                for fields in response.json()["content"]["survey"]:
                    column_name = fields.get("$xpath")
                    column_type = fields.get("type")
                    if column_name and column_type in allowed_types:
                        self.log.info(f"Name: {column_name}, type: {column_type}")
                        columns[column_name] = column_type
                        if column_type in ["geopoint", "geotrace", "geoshape"]:
                            geometric_columns.append(column_name)
                    elif column_name and column_type == "begin_repeat":
                        repeat_columns.append(fields.get("$xpath"))

                if columns:
                    columns.update({
                        "start": "datetime",
                        "end": "datetime",
                        "__version__": "text",
                        "_id": "integer",
                        "_uuid": "text",
                        "_submission_time": "datetime",
                        "_submitted_by": "text",
                        "_attachments": "text"
                    })

                return columns, geometric_columns, repeat_columns if columns else None
            else:
                self.log.error(f"Failed to getting form columns. Status code: {response.status_code}. {response.json()}")
                raise AirflowException(response.status_code, response.json())
        except Exception as e:
            raise AirflowException(str(e))

    def get_form_submission(self, formid:int, last_id: int = None) -> dict:
        """
        Retrieve submission of a specific form.
        You can specify "last_id" param that allows you to filter the sumbits with "_id" greater than the "last_id" specified

        :param formid: Form ID to be fetched
        :type formid: int

        :param last_id: Optional. filter the sumbits with "_id" greater than the "last_id" specified
        :type last_id: int

        Returns:
            A JSON object (or python dict) with the form submissions.
        """
        self.log.info(f"Form ID: {formid}")

        if last_id:
            self.log.info(f"Last ID: {last_id}")
            url = f"{self.kc_base_url}/api/v1/data/{formid}?query={{\"_id\": {{\"$gt\": {last_id}}}}}"
        else:
            url = f"{self.kc_base_url}/api/v1/data/{formid}"

        headers = {
            "Authorization": f"Token {self.token}"
            }

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                if response.text == "[]":
                    if last_id:
                        url = f"{self.kc_base_url}/api/v1/data/{formid}?query={{\"_id\": {{\"$lt\": {last_id}}}}}" # Check if for this form exist data for "_id" values less than "last_id".
                        response = requests.get(url, headers=headers)
                        if response.status_code == 200 and response.json(): # If there are values, all ok, not data to update.
                            self.log.warning(f'Form ID {formid} does not have data to retrieved for "_id" higher than {last_id}')
                            return None
                    else:                                              # If not, maybe it's a invalid or empty form.
                        self.log.error(f"Failed to retrieve form submission. Status code: {response.status_code} {response.text}")
                        raise Exception("Failed to retrieve form submission.")                
                else:
                    self.log.info("Form submission retrieved successfully.")
                    return response.json()
            else:
                self.log.error(f"Failed to retrieve form submission. Status code: {response.status_code} {response.text}")
                raise Exception("Failed to retrieve form submission.")
        except Exception as e:
            raise AirflowException(str(e))

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {
                "host": "Kobo Host",
                "login": "Kobo CAT Host",
                "password": "API Token",
            },
            "placeholders": {
                "host": 'KoboToolbox base url. For example "https://my-kobo.com"',
                "login": 'Kobo CAT base url. For example: "https://kc-mykobo.com',
                "password": "KoboToolBox access token",
            },
        }

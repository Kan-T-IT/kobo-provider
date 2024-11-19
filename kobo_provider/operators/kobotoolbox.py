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


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException, ParamValidationError, AirflowSkipException

from ..hooks.kobotoolbox import KoboHook
from geonode_provider.hooks.geonode import GeoNodeHook

import json
import re
from unidecode import unidecode
import pandas as pd
from sqlalchemy import create_engine, inspect, text
import psycopg2.errors
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon, MultiLineString
import os


class KoboToGeoNodeOperator(BaseOperator):
    """
    This operator integrates KoboToolBox with GeoNode, automating the migration of georeferenced forms to GeoNode.
    """

    template_fields = (
        'kobo_conn_id',
        'formid',
        'form_id_string',
        'postgres_conn_id',
        'geonode_conn_id',
        'columns',
        'dataset_name',
        'mode',
    )

    @apply_defaults
    def __init__(
        self,
        kobo_conn_id:str = None,
        formid:int = None,
        form_id_string:str = None,
        postgres_conn_id:str = None,
        geonode_conn_id:str = None,
        columns:list = None,
        dataset_name:str = None,
        mode:str = "append",
        *args,
        **kwargs,
    ):
        super(KoboToGeoNodeOperator, self).__init__(*args, **kwargs)
        if all([kobo_conn_id, postgres_conn_id, geonode_conn_id]) and any([formid, form_id_string]):
            self.kobo_conn_id = kobo_conn_id
            self.formid = formid
            self.form_id_string = form_id_string
            self.postgres_conn_id = postgres_conn_id
            self.geonode_conn_id = geonode_conn_id
            self.columns = columns # Optional
            self.dataset_name = dataset_name # Optional
            self.mode = mode # Optional
        else:
            raise ParamValidationError('Config parameters are not provided. Must to provide almost "kobo_conn_id", "formid" or "form_id_string", "postgres_conn_id", "geonode_conn_id".')

    def execute(self, context=None):
        if not (all([self.kobo_conn_id, self.postgres_conn_id, self.geonode_conn_id]) and any([self.formid, self.form_id_string])):
            raise ParamValidationError('Config parameters are not provided. Must to provide almost "kobo_conn_id", "formid" or "form_id_string", "postgres_conn_id", "geonode_conn_id".')

        kobo_conn = KoboHook(self.kobo_conn_id)

        if self.columns and "_id" not in self.columns:
            self.columns.append("_id")

        if self.form_id_string and not self.formid:
            self.formid, self.form_id_string = kobo_conn.get_form_ids(id_string=self.form_id_string)
        elif self.formid and not self.form_id_string:
            self.formid, self.form_id_string = kobo_conn.get_form_ids(formid=self.formid)

        form_columns, geometric_columns, repeat_columns = kobo_conn.get_form_columns(self.form_id_string)
        form_columns_names = []
        form_columns_names_repeat = set()
        for column in form_columns.keys():
            column = column.split("/")[-1]
            if column not in form_columns_names:
                form_columns_names.append(column)
            else:
                form_columns_names_repeat.add(column)
        if form_columns_names_repeat:
            self.log.error(f"Repeated columns name: {form_columns_names_repeat}")
            raise AirflowException("There cannot be columns with the same name.")

        if self.dataset_name:
            table = self.dataset_name
        else:
            table = kobo_conn.get_form_metadata(self.formid)["title"]

        table = unidecode(table.replace(" ", "_")).lower()

        if self.mode == "append":
            postgres_conn = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            postgres_connection = postgres_conn.get_conn()
            with postgres_connection.cursor() as cursor:
                try:
                    query = f"SELECT MAX(_id) FROM {table};"
                    cursor.execute(query)
                    last_id = cursor.fetchone()[0]
                    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' AND column_name NOT IN ('id', 'geom');"
                    cursor.execute(query)
                    columns = {record[0] for record in cursor.fetchall()}
                    postgres_connection.commit()

                    if (not self.columns and set(form_columns_names) != columns) or (self.columns and set(self.columns) != columns):
                        self.log.warning("The layer is already loaded but with other attributes/columns. The layer will be overwritten from the beginning with the new attributes/columns.")
                        columns_different = True
                        last_id = 0
                    else:
                        columns_different = False
                except psycopg2.errors.UndefinedTable:
                    last_id = 0
                    self.log.warning("The table/dataset does not exist.")
        else:
            last_id = 0

        postgres_connection = BaseHook.get_connection(self.postgres_conn_id)
        pg_username = postgres_connection.login
        pg_password = postgres_connection.password
        pg_host = postgres_connection.host
        pg_port = postgres_connection.port
        pg_dbname = postgres_connection.schema

        json_form = kobo_conn.get_form_submission(formid=self.formid, last_id=last_id)

        if not json_form:
            raise AirflowSkipException(f'Form ID {self.formid}/{self.form_id_string} does not have data to retrieved for "_id" higher than {last_id}. Skipping execution.')

        df = pd.DataFrame(json_form)
        attachment_columns = [column.split("/")[-1] for column, value in form_columns.items() if value in ["file", "image", "video", "audio"]]
        repeat_columns_copy = repeat_columns.copy()
        for column in df.columns:
            
            # Transform repeat fields responses
            for key in form_columns.keys():
                # if column_name in key and any(key.startswith(col) for col in repeat_columns): # Repeat columns 
                if key.startswith(column) and any(key.startswith(col) for col in repeat_columns): # Repeat columns
                    df = self.parse_repeat(column, df)
                    repeat_columns.remove(column)

        df = df.rename(columns=lambda x: x.split('/')[-1] if '/' in x else x)

        if self.columns:
            if attachment_columns and any(col in self.columns for col in attachment_columns) and "_attachments" not in self.columns: 
                self.columns.append("_attachments")
                attachments_needed = False
            elif "_attachments" in self.columns:
                attachments_needed = True
            else:
                attachments_needed = False
            try:
                df = df[self.columns]
            except KeyError as error:
                for col in self.columns:
                    if col in form_columns_names:
                        if col not in df.columns: # Happens when all submissions are null for a specific column. The API endpoint skips this column.
                            df[col] = None
                    else:
                        self.log.error(f"Error column: {col}")
                        raise KeyError("There are columns specified that do not exist in the form.")

                df = df[self.columns]
        else:
            for column in form_columns_names:
                if not column in df.columns:
                    df[column] = None
            df = df[form_columns_names]
            attachments_needed = True

        repeat_geometric_columns = list(filter(lambda col: col.startswith(tuple(repeat_columns_copy)), geometric_columns))
        repeat_geometric_columns = [col.split("/")[-1] for col in repeat_geometric_columns]
        repeat_geometric_columns = list(set(repeat_geometric_columns).intersection(df.columns))
        for geometric_column in repeat_geometric_columns:
            df = self.process_repeat_geometry_column(df, geometric_column)

        attachment_columns = list(set(attachment_columns).intersection(df.columns))
        if attachment_columns:
            df = df.apply(lambda row: self.process_attachments(row, df, attachment_columns), axis=1)

        if attachments_needed and not df["_attachments"].isna().all():
            df["_attachments"] = df["_attachments"].apply(self.parse_attachments)
        elif attachments_needed == False and "_attachments" in df.columns:
            df.drop(columns=["_attachments"], inplace=True)

        gdfs = []
        geographic_pattern = re.compile(r'^(-?\d+(\.\d+)?\s+-?\d+(\.\d+)?\s+0(\.0)?\s+0(\.0)?)(;(-?\d+(\.\d+)?\s+-?\d+(\.\d+)?\s+0(\.0)?\s+0(\.0)?))*$')
        for column in df.columns:
            if column in [col.split("/")[-1] for col in geometric_columns]:
                valid_rows = df[column].astype(str).apply(lambda x: bool(geographic_pattern.match(x)))
                if valid_rows.any():
                    df_copy = df[valid_rows].copy()
                    df_copy['geom'] = df_copy[column].apply(self.create_geometry)
                    gdfs.append(gpd.GeoDataFrame(df_copy, geometry='geom', crs="EPSG:4326"))

        if gdfs:
            gdf = pd.concat(gdfs, ignore_index=False)
        else:
            raise AirflowException("There are not geometry (coordinates) columns valid in this form.")

        gdf = gdf.sort_values(by='_id')
        gdf = gdf.set_crs('epsg:4326')

        self.log.info(f"Table preview:\n{gdf}")
        self.log.info(f"Table columns: {gdf.columns.tolist()}")

        try:
            engine = create_engine(f'postgresql://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_dbname}')
            geonode_hook = GeoNodeHook(geonode_conn_id=self.geonode_conn_id)
            inspector = inspect(engine)
            table_exists = inspector.has_table(table)

            if not table_exists or self.mode == "replace" or columns_different:
                if 'id' not in gdf.columns:
                    gdf.reset_index(drop=True, inplace=True)
                    gdf['id'] = gdf.index + 1
                    gdf.set_index('id', inplace=True)

                gdf.to_postgis(table, engine, if_exists="replace", index=False)

                with engine.connect() as connection:
                    create_sequence_sql = f"""
                        ALTER TABLE {table} ADD COLUMN id SERIAL PRIMARY KEY;
                    """
                    connection.execute(text(create_sequence_sql))
                    geonode_hook.publish_layer(layer_names=table)

            elif table_exists and self.mode == "append":
                if 'id' in gdf.columns:
                    gdf.drop(columns=['id'], inplace=True)

                gdf.to_postgis(table, engine, if_exists="append", index=False, index_label="id")

            self.log.info("GeoDataFrame successfully uploaded to GeoNode")
            geonode_hook.update_layer(layer_names=table)
            if not table_exists:
                geonode_hook.update_style(
                    style_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'style.sld'),
                    style_name=table
                )
            return
        except Exception as error:
            raise AirflowException("Error trying to upload GeoDataFrame to GeoNode", error)

    def parse_repeat(self, column, df):
        """
        This method process each row from repeat-type column (not include attachments columns) of a DataFrame and parse it to be displayed correctly in GeoNode.

        Returns:
            String with each response formated correctly.
        """
        df[column] = df[column].apply(lambda x: json.loads(json.dumps(x)) if isinstance(x, str) else x)  # Ensures that the data is in JSON format
        lists = df[column].apply(lambda x: x if isinstance(x, list) else [x])   # Extracts list of sub-objects

        # Create columns for each subobject
        sub_cols = set()
        for sublist in lists:
            if isinstance(sublist, list):
                for item in sublist:
                    sub_cols.update(item.keys())
            elif isinstance(sublist, dict):
                sub_cols.update(sublist.keys())

        # Create columns with lists of values
        for sub_col in sub_cols:
            df[sub_col] = lists.apply(lambda x: [item.get(sub_col, None) for item in x if item.get(sub_col, None) is not None] or None)

        df.drop(columns=[column], inplace=True) # Delete original column
        return df

    def process_repeat_geometry_column(self, df, column):

        df = df.explode(column).reset_index(drop=True)
        return df

    def process_attachments(self, row, df, attachment_columns):
        """
        Process each row to handle attachment columns in the DataFrame.

        For simple attachment columns (strings), replace the value with the download URL.

        For multiple attachment columns (lists), replace each value with the corresponding download URL, separated by spaces.
        """
        if row.get("_attachments"):
            for column in attachment_columns:
                if column in df.columns:
                    cell = row[column]

                    if isinstance(cell, str):
                        cell = cell.replace(" ", "_").replace("(", "").replace(")", "")

                        for attachment in row["_attachments"]:
                            attachment_name = attachment["filename"].split("/")[-1]
                            if cell == attachment_name:
                                row[column] = attachment["download_url"]
                                break

                    elif isinstance(cell, list):
                        cell = [filename.replace(" ", "_") for filename in cell]

                        urls = []
                        for filename in cell:
                            for attachment in row["_attachments"]:
                                attachment_name = attachment["filename"].split("/")[-1]
                                if filename == attachment_name:
                                    urls.append(attachment["download_url"])
                        row[column] = " ".join(urls)

        return row

    def parse_attachments(self, field) -> str:
        """
        This method process each row from attachments column of a DataFrame and parse it to be displayed correctly in GeoNode.

        Returns:
            String with each URL formated correctly.
        """
        if field and field not in ['""', '" "']:
            urls = None
            for attachment in field:
                for key, values in attachment.items():
                    if key == "download_url":
                        if urls:
                            urls = f"{urls} {values}"
                        else:
                            urls = values
            return urls
        else:
            return None

    def create_geometry(self, row):
        """
        This method process each row from coordinates column of a DataFrame and identifies which geometry type is and parse it to a standard geometric field.

        Returns:
            Standard geometric object.
        """
        coordinates = [tuple(map(float, point.split()[:2]))[::-1] for point in row.split(';')]
        if len(coordinates) <= 2 and len(coordinates) > 0:
            if len(coordinates) == 1:
                return Point(coordinates[0])
            elif len(coordinates) == 2:
                return LineString(coordinates)
        elif len(coordinates) > 2 and coordinates[0] != coordinates[-1]:
            return MultiLineString([LineString(coordinates)])
        elif len(coordinates) > 2 and coordinates[0] == coordinates[-1]:
            return Polygon(coordinates)

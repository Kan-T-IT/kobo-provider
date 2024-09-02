<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# KoboToolBox Provider for Apache Airflow
[![Linkedin](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://ar.linkedin.com/company/kan-territory-it)
[![Code style: black](https://img.shields.io/badge/Code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/:License-Apache%202-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)

[<img src="https://kan.com.ar/wp-content/uploads/2024/01/logoSM.png" alt="KAN" height="60" style="margin-right: 20px;">](https://kan.com.ar/)
[<img src="https://www.kobotoolbox.org/assets/images/common/kobotoolbox_logo_default_for-light-bg.svg" alt="Kobo" width="450" height="100">](https://www.kobotoolbox.org/)
[<img src="https://raw.githubusercontent.com/apache/airflow/19ebcac2395ef9a6b6ded3a2faa29dc960c1e635/docs/apache-airflow/img/logos/wordmark_1.png" alt="Airflow" width="250" height="100">](https://airflow.apache.org/)

<br>

## About
The following *Airflow Provider* is composed of an *Airflow Hook* named `KoboHook` and an Airflow Operator named `KoboToGeoNodeOperator`.\
The `Kobo Hook` has the purpose of performing the connection and basic operations with the KoboToolBox service. Within these operations it is possible:
* Get the `formid` of a form from his `id_string` and vice versa
* Get form structure
* Get metadata from a form 
* Extract forms submissions
* More features are in development

The `KoboToGeoNodeOperator` is an operator focused on those forms that have geospatial information that needs to be displayed in GeoNode. This operator allows to capture the information of each response, that a user makes in a given form, and add them to an existing dataset. If it is a new form, the Operator will create the new dataset with the first response.
For the mapping of columns and certain necessary and complementary information, the Operator uses a certain number of parameters which are defined when instantiating the Operator.

## Contents
* [Installation](#installation)
* [Configuration](#configuration)
    * [Connections](#connections)
        * [Kobo connection](#kobo-connection)
        * [Postgres connection](#postgres-connection)
    * [Configuration parameters](#configuration-parameters)
        * [In task body](#in-task-body)
        * [Config file](#config-file)
* [Example DAG](#example-dag)

<br>

## Installation
Installation of `airflow_providers_geonode` is needed to use `KoboToGeoNodeOperator`.

## Configuration
To use the `KoboToGeoNodeOperator` you will first need to make some configurations:


### Connections
In the Airflow UI three connections must be created:
#### Kobo connection
To connect to the KoboToolBox service, a `Connection ID` is required along with the `Kobo Host`, `Kobo CAT Host` (which has access to API v1) and `API token`.\
Note: It is important to note that the user generating the token must have access to the specific form in question.

<p align="center">
  <img src="https://kan.com.ar/wp-content/uploads/2024/08/kobo-connetion.png" alt="Kobo Connection" height="450px">
</p>

#### Postgres connection
It will be necessary to connect Airflow with the destination GeoNode database to which you want to migrate the form data. For this it will be necessary to specify the following parameters related to the database:
* `Connection Id`: Connection name in Airflow
* `Host`: Database host
* `Database`: Database name (typically *geonode_data*)
* `Login`: Database username
* `Password`: Database password
* `Port`: Database port

<p align="center" style="margin-top: 20px;">
  <img src="https://kan.com.ar/wp-content/uploads/2024/04/postgres_connection.png" alt="Postgres Connection" height="550px">
</p>

#### GeoNode connection
It will be necessary to connect Airflow with the destination GeoNode to which you want to upload the form data. For this it will be necessary to specify the following parameters related to the GeoNode instance:
* `Connection Id`: Connection name in Airflow.
* `Base URL`: URL/Host to GeoNode with *https* or *http* as appropiate.
* `GeoNode username`: GeoNode app username (*optional if GeoNode API Token has been placed and GeoServer credentials will be defined bellow*).
* `GeoNode password`: GeoNode app password (*optional if GeoNode API Token has been placed and GeoServer credentials will be defined bellow*).
* `GeoNode API Token`: It is possible to work in GeoNode by authenticating with an API Token (*optional if GeoNode username and password has been placed*).
* `GeoServer username`: GeoServer app username (*Optional. If GeoServer credentials differ from GeoNode credentials you must to specify them. Otherwise, you can specify only the GeoNode credentials*).
* `GeoServer password`: GeoServer app password (*Optional. If GeoServer credentials differ from GeoNode credentials you must to specify them. Otherwise, you can specify only the GeoNode credentials*).
* `SSH Host`: SSH host where the GeoNode instance is hosted.
* `SSH port`: SSH port where the GeoNode instance is hosted.
* `SSH username`: SSH username where the GeoNode instance is hosted.
* `SSH password`: SSH password where the GeoNode instance is hosted.

<p align="center" style="margin-top: 20px;">
  <img src="https://kan.com.ar/wp-content/uploads/2024/05/geonode_airflow_conn.png" alt="GeoNode Connection" height="550px">
</p>

<br>

### Configuration parameters
To use the `KoboToGeoNodeOperator` operator it is necessary to configure some required parameters and others will be optional.

The parameters can be defined in the body of the DAG, when instantiating the Operator, as is normally done:
```python
process_form = KoboToGeoNodeOperator(
    task_id="process_form",
    kobo_conn_id="kobo_connection_id_airflow",
    formid=39,                               # This or
    form_id_string="y4nMrHxChj3XZEQLVDwQIN", # This
    postgres_conn_id="postgres_connection_id_airflow",
    geonode_conn_id="geonode_connection_id_airflow",
    dataset_name= "dataset_name_in_geonode", # Optional
    columns=[                                # Optional
        "username",
        "column_1",
        "column_2",
        "column_3",
        "column_4",
        "column_5",
        "my_coords_column"
    ],
    mode="append/replace", # Optional
    dag=dag
)
```

### Parameters:
* `kobo_conn_id` *Required*: The name of the Kobo connection [previously created in the Airflow UI](#kobo-connection).
* `formid` *Required*: ID of the Kobo form from which you want to extract the information. Instead of this parameter the following one can be defined (*form_id_string*).
* `form_id_string` *Required*: ID string the Kobo form from which you want to extract the information. Instead of this parameter the previous one can be defined (*formid*).
* `postgres_conn_id` *Required*: The name of the connection to the target database (GeoNode) [previously created in the Airflow UI](#postgres-connection).
* `geonode_conn_id` *Required*: The name of the GeoNode connection [previously created in the Airflow UI](#geonode-connection).
* `dataset_name` *Optional*: The name that the dataset will have (or has, if it already exists) where you want to create/add the form information. Check that name has not special characters, otherwise these will be converted to the closest equivalent in ASCII system. If it not defined, the dataset will take the form name.
* `columns` *Optional*: A list or array with the name of the columns to migrate. If this parameter is not defined, all columns of the form (including metadata) will be migrated to GeoNode.
* `mode` *Optional*: Specifies whether the data to be inserted will be added to the existing data (`append`) or whether it will overwrite the old data (`replace`). By default (If it not defined) data will be append to the previous data.

<br>

## Example DAG
A DAG template of how the `KoboToGeoNodeOperator` could be used is provided inside the "*example-dag*" folder.

Then the DAG would look something like this:
```python
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from kobo_provider.operators.kobotoolbox import KoboToGeoNodeOperator


default_args = {
    'owner': 'KAN',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    "KoboToGeoNodeOperator-ExampleDAG",
    default_args=default_args,
    description='ETL Kobo-GeoNode',
    schedule_interval=None,
    tags=['KoboToolBox', 'GeoNode']
)

start = EmptyOperator(
    task_id='start',
    dag=dag
)

process_form = KoboToGeoNodeOperator(
    task_id="process_form",
    kobo_conn_id="kobo_connection_id_airflow",
    formid=39,                               # This or
    form_id_string="y4nMrHxChj3XZEQLVDwQIN", # This
    postgres_conn_id="postgres_connection_id_airflow",
    geonode_conn_id="geonode_connection_id_airflow",
    dataset_name= "dataset_name_in_geonode", # Optional
    columns=[                                # Optional
        "username",
        "column_1",
        "column_2",
        "column_3",
        "column_4",
        "column_5",
        "my_coords_column"
    ],
    mode="replace", # Optional
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

start >> process_form >> end
```
You can also take the already made DAG from the [mentioned folder](#example-dag).




<br>

---
© 2024 KAN Territory & IT. All rights reserved.
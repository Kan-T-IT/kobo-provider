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

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE
# OVERWRITTEN WHEN PREPARING PACKAGES.
#
# IF YOU WANT TO MODIFY THIS FILE, YOU SHOULD MODIFY THE TEMPLATE
# `get_provider_info_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze/templates` DIRECTORY


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-smtp",
        "name": "Simple Mail Transfer Protocol (SMTP)",
        "description": "`Simple Mail Transfer Protocol (SMTP) <https://tools.ietf.org/html/rfc5321>`__\n",
        "state": "ready",
        "source-date-epoch": 1723970570,
        "versions": [
            "1.8.0",
            "1.7.1",
            "1.7.0",
            "1.6.1",
            "1.6.0",
            "1.5.0",
            "1.4.1",
            "1.4.0",
            "1.3.2",
            "1.3.1",
            "1.3.0",
            "1.2.0",
            "1.1.0",
            "1.0.1",
            "1.0.0",
        ],
        "dependencies": ["apache-airflow>=2.8.0"],
        "integrations": [
            {
                "integration-name": "Simple Mail Transfer Protocol (SMTP)",
                "external-doc-url": "https://tools.ietf.org/html/rfc5321",
                "logo": "/integration-logos/smtp/SMTP.png",
                "tags": ["protocol"],
            }
        ],
        "operators": [
            {
                "integration-name": "Simple Mail Transfer Protocol (SMTP)",
                "python-modules": ["airflow.providers.smtp.operators.smtp"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Simple Mail Transfer Protocol (SMTP)",
                "python-modules": ["airflow.providers.smtp.hooks.smtp"],
            }
        ],
        "connection-types": [
            {"hook-class-name": "airflow.providers.smtp.hooks.smtp.SmtpHook", "connection-type": "smtp"}
        ],
        "notifications": ["airflow.providers.smtp.notifications.smtp.SmtpNotifier"],
        "config": {
            "smtp_provider": {
                "description": "Options for SMTP provider.",
                "options": {
                    "ssl_context": {
                        "description": 'ssl context to use when using SMTP and IMAP SSL connections. By default, the context is "default"\nwhich sets it to ``ssl.create_default_context()`` which provides the right balance between\ncompatibility and security, it however requires that certificates in your operating system are\nupdated and that SMTP/IMAP servers of yours have valid certificates that have corresponding public\nkeys installed on your machines. You can switch it to "none" if you want to disable checking\nof the certificates, but it is not recommended as it allows MITM (man-in-the-middle) attacks\nif your infrastructure is not sufficiently secured. It should only be set temporarily while you\nare fixing your certificate configuration. This can be typically done by upgrading to newer\nversion of the operating system you run Airflow components on,by upgrading/refreshing proper\ncertificates in the OS or by updating certificates for your mail servers.\n\nIf you do not set this option explicitly, it will use Airflow "email.ssl_context" configuration,\nbut if this configuration is not present, it will use "default" value.\n',
                        "type": "string",
                        "version_added": "1.3.0",
                        "example": "default",
                        "default": None,
                    },
                    "templated_email_subject_path": {
                        "description": "Allows overriding of the standard templated email subject line when the SmtpNotifier is used.\nMust provide a path to the template.\n",
                        "type": "string",
                        "version_added": "1.6.1",
                        "example": "path/to/override/email_subject.html",
                        "default": None,
                    },
                    "templated_html_content_path": {
                        "description": "Allows overriding of the standard templated email path when the SmtpNotifier is used. Must provide\na path to the template.\n",
                        "type": "string",
                        "version_added": "1.6.1",
                        "example": "path/to/override/email.html",
                        "default": None,
                    },
                },
            }
        },
    }

import logging
import os
import csv
import random
from pyathena.util import parse_output_location

from redash.query_runner import *
from redash.settings import parse_boolean
from redash.utils import json_dumps, json_loads

logger = logging.getLogger(__name__)
ANNOTATE_QUERY = parse_boolean(os.environ.get('ATHENA_ANNOTATE_QUERY', 'true'))
ANNOTATE_QUERY_FOR_DML = parse_boolean(os.environ.get('ATHENA_ANNOTATE_QUERY_FOR_DML', 'true'))
SHOW_EXTRA_SETTINGS = parse_boolean(os.environ.get('ATHENA_SHOW_EXTRA_SETTINGS', 'true'))
ASSUME_ROLE = parse_boolean(os.environ.get('ATHENA_ASSUME_ROLE', 'false'))
OPTIONAL_CREDENTIALS = parse_boolean(os.environ.get('ATHENA_OPTIONAL_CREDENTIALS', 'true'))

try:
    import pyathena
    import boto3

    enabled = True
except ImportError:
    enabled = False

_TYPE_MAPPINGS = {
    'boolean': TYPE_BOOLEAN,
    'tinyint': TYPE_INTEGER,
    'smallint': TYPE_INTEGER,
    'integer': TYPE_INTEGER,
    'bigint': TYPE_INTEGER,
    'double': TYPE_FLOAT,
    'varchar': TYPE_STRING,
    'timestamp': TYPE_DATETIME,
    'date': TYPE_DATE,
    'varbinary': TYPE_STRING,
    'array': TYPE_STRING,
    'map': TYPE_STRING,
    'row': TYPE_STRING,
    'decimal': TYPE_FLOAT,
}


class SimpleFormatter(object):
    def format(self, operation, parameters=None):
        return operation


class Athena(BaseQueryRunner):
    noop_query = 'SELECT 1'

    @classmethod
    def name(cls):
        return "Amazon Athena"

    @classmethod
    def configuration_schema(cls):
        schema = {
            'type': 'object',
            'properties': {
                'region': {
                    'type': 'string',
                    'title': 'AWS Region'
                },
                'aws_access_key': {
                    'type': 'string',
                    'title': 'AWS Access Key'
                },
                'aws_secret_key': {
                    'type': 'string',
                    'title': 'AWS Secret Key'
                },
                's3_staging_dir': {
                    'type': 'string',
                    'title': 'S3 Staging (Query Results) Bucket Path'
                },
                'schema': {
                    'type': 'string',
                    'title': 'Schema Name',
                    'default': 'default'
                },
                'glue': {
                    'type': 'boolean',
                    'title': 'Use Glue Data Catalog',
                },
                'work_group': {
                    'type': 'string',
                    'title': 'Athena Work Group',
                    'default': 'primary'
                },
            },
            'required': ['region', 's3_staging_dir'],
            'order': ['region', 's3_staging_dir', 'schema', 'work_group'],
            'secret': ['aws_secret_key']
        }

        if SHOW_EXTRA_SETTINGS:
            schema['properties'].update({
                'encryption_option': {
                    'type': 'string',
                    'title': 'Encryption Option',
                },
                'kms_key': {
                    'type': 'string',
                    'title': 'KMS Key',
                },
            })

        if ASSUME_ROLE:
            del schema['properties']['aws_access_key']
            del schema['properties']['aws_secret_key']
            schema['secret'] = []

            schema['order'].insert(1, 'iam_role')
            schema['order'].insert(2, 'external_id')
            schema['properties'].update({
                'iam_role': {
                    'type': 'string',
                    'title': 'IAM role to assume',
                },
                'external_id': {
                    'type': 'string',
                    'title': 'External ID to be used while STS assume role',
                },
            })
        else:
            schema['order'].insert(1, 'aws_access_key')
            schema['order'].insert(2, 'aws_secret_key')

        if not OPTIONAL_CREDENTIALS and not ASSUME_ROLE:
            schema['required'] += ['aws_access_key', 'aws_secret_key']

        return schema

    @classmethod
    def enabled(cls):
        return enabled

    def annotate_query(self, query, metadata):
        if ANNOTATE_QUERY:
            if ANNOTATE_QUERY_FOR_DML:
                return super(Athena, self).annotate_query_with_single_line_comment(query, metadata)
            else:
                return super(Athena, self).annotate_query(query, metadata)
        return query

    @classmethod
    def type(cls):
        return "athena"

    def _get_iam_credentials(self, user=None):
        if ASSUME_ROLE:
            role_session_name = 'redash' if user is None else user.email
            sts = boto3.client('sts')
            creds = sts.assume_role(
                RoleArn=self.configuration.get('iam_role'),
                RoleSessionName=role_session_name,
                ExternalId=self.configuration.get('external_id')
            )
            return {
                'aws_access_key_id': creds['Credentials']['AccessKeyId'],
                'aws_secret_access_key': creds['Credentials']['SecretAccessKey'],
                'aws_session_token': creds['Credentials']['SessionToken'],
                'region_name': self.configuration['region']
            }
        else:
            return {
                'aws_access_key_id': self.configuration.get('aws_access_key', None),
                'aws_secret_access_key': self.configuration.get('aws_secret_key', None),
                'region_name': self.configuration['region']
            }

    def __get_schema_from_glue(self):
        client = boto3.client('glue', **self._get_iam_credentials())
        schema = {}

        database_paginator = client.get_paginator('get_databases')
        table_paginator = client.get_paginator('get_tables')

        for databases in database_paginator.paginate():
            for database in databases['DatabaseList']:
                iterator = table_paginator.paginate(DatabaseName=database['Name'])
                for table in iterator.search('TableList[]'):
                    table_name = '%s.%s' % (database['Name'], table['Name'])
                    if table_name not in schema:
                        column = [columns['Name'] for columns in table['StorageDescriptor']['Columns']]
                        schema[table_name] = {'name': table_name, 'columns': column}
                        for partition in table.get('PartitionKeys', []):
                            schema[table_name]['columns'].append(partition['Name'])
        return schema.values()

    def get_schema(self, get_stats=False):
        if self.configuration.get('glue', False):
            return self.__get_schema_from_glue()

        schema = {}
        query = """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema')
        """

        results, error = self.run_query(query, None)
        if error is not None:
            raise Exception("Failed getting schema.")

        results = json_loads(results)
        for row in results['rows']:
            table_name = '{0}.{1}'.format(row['table_schema'], row['table_name'])
            if table_name not in schema:
                schema[table_name] = {'name': table_name, 'columns': []}
            schema[table_name]['columns'].append(row['column_name'])

        return schema.values()

    def run_query(self, query, user):
        cursor = pyathena.connect(
            s3_staging_dir=self.configuration['s3_staging_dir'],
            schema_name=self.configuration.get('schema', 'default'),
            encryption_option=self.configuration.get('encryption_option', None),
            kms_key=self.configuration.get('kms_key', None),
            work_group=self.configuration.get('work_group', 'primary'),
            formatter=SimpleFormatter(),
            **self._get_iam_credentials(user=user)).cursor()
        cursor.execute(query)

        return self.get_query_result_from_file(cursor, user)
        # return self.get_query_results_from_cursor(cursor)

    def get_query_results_from_cursor(self, cursor):
        try:
            column_tuples = [(i[0], _TYPE_MAPPINGS.get(i[1], None)) for i in cursor.description]
            columns = self.fetch_columns(column_tuples)
            rows = [dict(zip(([c['name'] for c in columns]), r)) for i, r in enumerate(cursor.fetchall())]
            qbytes = None
            athena_query_id = None
            try:
                qbytes = cursor.data_scanned_in_bytes
            except AttributeError as e:
                logger.debug("Athena Upstream can't get data_scanned_in_bytes: %s", e)
            try:
                athena_query_id = cursor.query_id
            except AttributeError as e:
                logger.debug("Athena Upstream can't get query_id: %s", e)
            data = {
                'columns': columns,
                'rows': rows,
                'metadata': {
                    'data_scanned': qbytes,
                    'athena_query_id': athena_query_id
                }
            }
            json_data = json_dumps(data, ignore_nan=True)
            error = None
        except (KeyboardInterrupt, InterruptException):
            if cursor.query_id:
                cursor.cancel()
            error = "Query cancelled by user."
            json_data = None
        except Exception as ex:
            if cursor.query_id:
                cursor.cancel()
            error = ex.message
            json_data = None
        return json_data, error

    def get_query_result_from_file(self, cursor, user):
        try:
            qbytes = None
            athena_query_results_file = None
            error = None
            json_data = None
            try:
                athena_query_id = cursor.query_id
            except AttributeError as e:
                athena_query_id = "temp_"+str(random.getrandbits(128))
                logger.debug("Athena Upstream can't get query_id: %s", e)
            try:
                athena_output_location = cursor.output_location
            except Exception as e:
                error = e.message
                logger.debug("Output location not found: %s", e)
                return json_data, error

            bucket, key = parse_output_location(athena_output_location)
            s3 = boto3.client('s3', **self._get_iam_credentials(user=user))
            athena_query_results_file = athena_query_id
            with open(athena_query_results_file, 'wb') as w:
                s3.download_fileobj(bucket, key, w)
            with open(athena_query_results_file, 'r+') as f:
                rows = list(csv.DictReader(f))
            column_tuples = [(i[0], _TYPE_MAPPINGS.get(i[1], None)) for i in cursor.description]
            columns = self.fetch_columns(column_tuples)
            try:
                qbytes = cursor.data_scanned_in_bytes
            except AttributeError as e:
                logger.debug("Athena Upstream can't get data_scanned_in_bytes: %s", e)
            data = {
                'columns': columns,
                'rows': rows,
                'metadata': {
                    'data_scanned': qbytes,
                    'athena_query_id': athena_query_id
                }
            }
            json_data = json_dumps(data, ignore_nan=True)
        except (KeyboardInterrupt, InterruptException) as e:
            if cursor.query_id:
                cursor.cancel()
            error = "Query cancelled by user."
            json_data = None
        except Exception as ex:
            if cursor.query_id:
                cursor.cancel()
                logger.debug(ex.message)
            error = ex
            json_data = None
        finally:
            self.remove_file(athena_query_results_file)

        self.remove_file(athena_query_results_file)
        return json_data, error

    def remove_file(self, athena_query_results_file):
        try:
            os.remove(athena_query_results_file)
        except OSError:
            logger.debug("No such file with %s exists", athena_query_results_file)


register(Athena)

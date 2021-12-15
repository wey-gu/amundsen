# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script demonstrating how to load data into Nebula and
Elasticsearch without using an Airflow DAG.

It contains several jobs:
- `run_csv_job`: runs a job that extracts table data from a CSV, loads (writes)
  this into a different local directory as a csv, then publishes this data to
  nebula.
- `run_table_column_job`: does the same thing as `run_csv_job`, but with a csv
  containing column data.
- `create_last_updated_job`: creates a job that gets the current time, dumps it
  into a predefined model schema, and publishes this to nebula.
- `create_es_publisher_sample_job`: creates a job that extracts data from nebula
  and pubishes it into elasticsearch.

For other available extractors, please take a look at
https://github.com/amundsen-io/amundsendatabuilder#list-of-extractors
"""

import logging
import os
import sys
import uuid

from amundsen_common.models.index_map import DASHBOARD_ELASTICSEARCH_INDEX_MAPPING, USER_INDEX_MAP
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.csv_extractor import (
    CsvColumnLineageExtractor, CsvExtractor, CsvTableBadgeExtractor, CsvTableColumnExtractor, CsvTableLineageExtractor,
)
from databuilder.extractor.es_last_updated_extractor import EsLastUpdatedExtractor
# from databuilder.extractor.nebula_search_data_extractor import NebulaSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_nebula_csv_loader import FsNebulaCSVLoader
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.nebula_csv_publisher import NebulaCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import ChainedTransformer, NoopTransformer
from databuilder.transformer.dict_to_model import MODEL_CLASS, DictToModel
from databuilder.transformer.generic_transformer import (
    CALLBACK_FUNCTION, FIELD_NAME, GenericTransformer,
)

es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', 'localhost')
NEBULA_ENDPOINTS = os.getenv('CREDENTIALS_nebula_endpoints', 'localhost:9669')

es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)

if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    nebula_endpoints = sys.argv[2]

es = Elasticsearch([
    {'host': es_host, 'port': es_port},
])

Base = declarative_base()

nebula_endpoints = NEBULA_ENDPOINTS

nebula_user = 'root'
nebula_password = 'nebula'

LOGGER = logging.getLogger(__name__)


def run_csv_job(file_loc, job_name, model):
    tmp_folder = f'/var/tmp/amundsen/{job_name}'
    vertex_files_folder = f'{tmp_folder}/nodes'
    edge_dir_files_folder = f'{tmp_folder}/relationships'

    csv_extractor = CsvExtractor()
    csv_loader = FsNebulaCSVLoader()

    task = DefaultTask(extractor=csv_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        'extractor.csv.file_location': file_loc,
        'extractor.csv.model_class': model,
        'loader.filesystem_csv_nebula.vertex_dir_path': vertex_files_folder,
        'loader.filesystem_csv_nebula.edge_dir_path': edge_dir_files_folder,
        'loader.filesystem_csv_nebula.delete_created_directories': True,
        'publisher.nebula.vertex_files_directory': vertex_files_folder,
        'publisher.nebula.edge_files_directory': edge_dir_files_folder,
        'publisher.nebula.nebula_endpoints': nebula_endpoints,
        'publisher.nebula.nebula_user': nebula_user,
        'publisher.nebula.nebula_password': nebula_password,
        'publisher.nebula.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })

    DefaultJob(conf=job_config,
               task=task,
               publisher=NebulaCsvPublisher()).launch()


def run_table_badge_job(table_path, badge_path):
    tmp_folder = '/var/tmp/amundsen/table_badge'
    vertex_files_folder = f'{tmp_folder}/nodes'
    edge_dir_files_folder = f'{tmp_folder}/relationships'
    extractor = CsvTableBadgeExtractor()
    csv_loader = FsNebulaCSVLoader()
    task = DefaultTask(extractor=extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())
    job_config = ConfigFactory.from_dict({
        'extractor.csvtablebadge.table_file_location': table_path,
        'extractor.csvtablebadge.badge_file_location': badge_path,
        'loader.filesystem_csv_nebula.vertex_dir_path': vertex_files_folder,
        'loader.filesystem_csv_nebula.edge_dir_path': edge_dir_files_folder,
        'loader.filesystem_csv_nebula.delete_created_directories': True,
        'publisher.nebula.vertex_files_directory': vertex_files_folder,
        'publisher.nebula.edge_files_directory': edge_dir_files_folder,
        'publisher.nebula.nebula_endpoints': nebula_endpoints,
        'publisher.nebula.nebula_user': nebula_user,
        'publisher.nebula.nebula_password': nebula_password,
        'publisher.nebula.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=NebulaCsvPublisher())
    job.launch()


def run_table_column_job(table_path, column_path):
    tmp_folder = '/var/tmp/amundsen/table_column'
    vertex_files_folder = f'{tmp_folder}/nodes'
    edge_dir_files_folder = f'{tmp_folder}/relationships'
    extractor = CsvTableColumnExtractor()
    csv_loader = FsNebulaCSVLoader()
    task = DefaultTask(extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())
    job_config = ConfigFactory.from_dict({
        'extractor.csvtablecolumn.table_file_location': table_path,
        'extractor.csvtablecolumn.column_file_location': column_path,
        'loader.filesystem_csv_nebula.vertex_dir_path': vertex_files_folder,
        'loader.filesystem_csv_nebula.edge_dir_path': edge_dir_files_folder,
        'loader.filesystem_csv_nebula.delete_created_directories': True,
        'publisher.nebula.vertex_files_directory': vertex_files_folder,
        'publisher.nebula.edge_files_directory': edge_dir_files_folder,
        'publisher.nebula.nebula_endpoints': nebula_endpoints,
        'publisher.nebula.nebula_user': nebula_user,
        'publisher.nebula.nebula_password': nebula_password,
        'publisher.nebula.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=NebulaCsvPublisher())
    job.launch()


def run_table_lineage_job(table_lineage_path):
    tmp_folder = '/var/tmp/amundsen/table_column'
    vertex_files_folder = f'{tmp_folder}/nodes'
    edge_dir_files_folder = f'{tmp_folder}/relationships'
    extractor = CsvTableLineageExtractor()
    csv_loader = FsNebulaCSVLoader()
    task = DefaultTask(extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())
    job_config = ConfigFactory.from_dict({
        'extractor.csvtablelineage.table_lineage_file_location': table_lineage_path,
        'loader.filesystem_csv_nebula.vertex_dir_path': vertex_files_folder,
        'loader.filesystem_csv_nebula.edge_dir_path': edge_dir_files_folder,
        'loader.filesystem_csv_nebula.delete_created_directories': True,
        'publisher.nebula.vertex_files_directory': vertex_files_folder,
        'publisher.nebula.edge_files_directory': edge_dir_files_folder,
        'publisher.nebula.nebula_endpoints': nebula_endpoints,
        'publisher.nebula.nebula_user': nebula_user,
        'publisher.nebula.nebula_password': nebula_password,
        'publisher.nebula.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=NebulaCsvPublisher())
    job.launch()


def run_column_lineage_job(column_lineage_path):
    tmp_folder = '/var/tmp/amundsen/table_column'
    vertex_files_folder = f'{tmp_folder}/nodes'
    edge_dir_files_folder = f'{tmp_folder}/relationships'
    extractor = CsvColumnLineageExtractor()
    csv_loader = FsNebulaCSVLoader()
    task = DefaultTask(extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())
    job_config = ConfigFactory.from_dict({
        'extractor.csvcolumnlineage.column_lineage_file_location': column_lineage_path,
        'loader.filesystem_csv_nebula.vertex_dir_path': vertex_files_folder,
        'loader.filesystem_csv_nebula.edge_dir_path': edge_dir_files_folder,
        'loader.filesystem_csv_nebula.delete_created_directories': True,
        'publisher.nebula.vertex_files_directory': vertex_files_folder,
        'publisher.nebula.edge_files_directory': edge_dir_files_folder,
        'publisher.nebula.nebula_endpoints': nebula_endpoints,
        'publisher.nebula.nebula_user': nebula_user,
        'publisher.nebula.nebula_password': nebula_password,
        'publisher.nebula.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=NebulaCsvPublisher())
    job.launch()


def create_last_updated_job():
    # loader saves data to these folders and publisher reads it from here
    tmp_folder = '/var/tmp/amundsen/last_updated_data'
    vertex_files_folder = f'{tmp_folder}/nodes'
    edge_dir_files_folder = f'{tmp_folder}/relationships'

    task = DefaultTask(extractor=EsLastUpdatedExtractor(),
                       loader=FsNebulaCSVLoader())

    job_config = ConfigFactory.from_dict({
        'extractor.es_last_updated.model_class':
            'databuilder.models.es_last_updated.ESLastUpdated',

        'loader.filesystem_csv_nebula.vertex_dir_path': vertex_files_folder,
        'loader.filesystem_csv_nebula.edge_dir_path': edge_dir_files_folder,
        'publisher.nebula.vertex_files_directory': vertex_files_folder,
        'publisher.nebula.edge_files_directory': edge_dir_files_folder,
        'publisher.nebula.nebula_endpoints': nebula_endpoints,
        'publisher.nebula.nebula_user': nebula_user,
        'publisher.nebula.nebula_password': nebula_password,
        'publisher.nebula.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })

    return DefaultJob(conf=job_config,
                      task=task,
                      publisher=NebulaCsvPublisher())


def _str_to_list(str_val):
    return str_val.split(',')


def create_dashboard_tables_job():
    # loader saves data to these folders and publisher reads it from here
    tmp_folder = '/var/tmp/amundsen/dashboard_table'
    vertex_files_folder = f'{tmp_folder}/nodes'
    edge_dir_files_folder = f'{tmp_folder}/relationships'

    csv_extractor = CsvExtractor()
    csv_loader = FsNebulaCSVLoader()

    generic_transformer = GenericTransformer()
    dict_to_model_transformer = DictToModel()
    transformer = ChainedTransformer(transformers=[generic_transformer, dict_to_model_transformer],
                                     is_init_transformers=True)

    task = DefaultTask(extractor=csv_extractor,
                       loader=csv_loader,
                       transformer=transformer)
    publisher = NebulaCsvPublisher()

    job_config = ConfigFactory.from_dict({
        f'{csv_extractor.get_scope()}.file_location': 'example/sample_data/sample_dashboard_table.csv',
        f'{transformer.get_scope()}.{generic_transformer.get_scope()}.{FIELD_NAME}': 'table_ids',
        f'{transformer.get_scope()}.{generic_transformer.get_scope()}.{CALLBACK_FUNCTION}': _str_to_list,
        f'{transformer.get_scope()}.{dict_to_model_transformer.get_scope()}.{MODEL_CLASS}':
            'databuilder.models.dashboard.dashboard_table.DashboardTable',
        f'{csv_loader.get_scope()}.vertex_dir_path': vertex_files_folder,
        f'{csv_loader.get_scope()}.edge_dir_path': edge_dir_files_folder,
        f'{csv_loader.get_scope()}.delete_created_directories': True,
        f'{publisher.get_scope()}.vertex_files_directory': vertex_files_folder,
        f'{publisher.get_scope()}.edge_files_directory': edge_dir_files_folder,
        f'{publisher.get_scope()}.nebula_endpoints': nebula_endpoints,
        f'{publisher.get_scope()}.nebula_user': nebula_user,
        f'{publisher.get_scope()}.nebula_password': nebula_password,
        f'{publisher.get_scope()}.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })

    return DefaultJob(conf=job_config,
                      task=task,
                      publisher=publisher)


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    logging.basicConfig(level=logging.DEBUG)

    run_table_column_job('example/sample_data/sample_table.csv', 'example/sample_data/sample_col.csv')
    run_table_badge_job('example/sample_data/sample_table.csv', 'example/sample_data/sample_badges.csv')
    run_table_lineage_job('example/sample_data/sample_table_lineage.csv')
    run_column_lineage_job('example/sample_data/sample_column_lineage.csv')
    run_csv_job('example/sample_data/sample_table_column_stats.csv', 'test_table_column_stats',
                'databuilder.models.table_stats.TableColumnStats')
    run_csv_job('example/sample_data/sample_table_programmatic_source.csv', 'test_programmatic_source',
                'databuilder.models.table_metadata.TableMetadata')
    run_csv_job('example/sample_data/sample_watermark.csv', 'test_watermark_metadata',
                'databuilder.models.watermark.Watermark')
    run_csv_job('example/sample_data/sample_table_owner.csv', 'test_table_owner_metadata',
                'databuilder.models.table_owner.TableOwner')
    run_csv_job('example/sample_data/sample_column_usage.csv', 'test_usage_metadata',
                'databuilder.models.table_column_usage.ColumnReader')
    run_csv_job('example/sample_data/sample_user.csv', 'test_user_metadata',
                'databuilder.models.user.User')
    run_csv_job('example/sample_data/sample_application.csv', 'test_application_metadata',
                'databuilder.models.application.Application')
    run_csv_job('example/sample_data/sample_table_report.csv', 'test_report_metadata',
                'databuilder.models.report.ResourceReport')
    run_csv_job('example/sample_data/sample_source.csv', 'test_source_metadata',
                'databuilder.models.table_source.TableSource')
    run_csv_job('example/sample_data/sample_tags.csv', 'test_tag_metadata',
                'databuilder.models.table_metadata.TagMetadata')
    run_csv_job('example/sample_data/sample_table_last_updated.csv', 'test_table_last_updated_metadata',
                'databuilder.models.table_last_updated.TableLastUpdated')
    run_csv_job('example/sample_data/sample_schema_description.csv', 'test_schema_description',
                'databuilder.models.schema.schema.SchemaModel')
    run_csv_job('example/sample_data/sample_dashboard_base.csv', 'test_dashboard_base',
                'databuilder.models.dashboard.dashboard_metadata.DashboardMetadata')
    run_csv_job('example/sample_data/sample_dashboard_usage.csv', 'test_dashboard_usage',
                'databuilder.models.dashboard.dashboard_usage.DashboardUsage')
    run_csv_job('example/sample_data/sample_dashboard_owner.csv', 'test_dashboard_owner',
                'databuilder.models.dashboard.dashboard_owner.DashboardOwner')
    run_csv_job('example/sample_data/sample_dashboard_query.csv', 'test_dashboard_query',
                'databuilder.models.dashboard.dashboard_query.DashboardQuery')
    run_csv_job('example/sample_data/sample_dashboard_last_execution.csv', 'test_dashboard_last_execution',
                'databuilder.models.dashboard.dashboard_execution.DashboardExecution')
    run_csv_job('example/sample_data/sample_dashboard_last_modified.csv', 'test_dashboard_last_modified',
                'databuilder.models.dashboard.dashboard_last_modified.DashboardLastModifiedTimestamp')

    create_dashboard_tables_job().launch()

    create_last_updated_job().launch()

    # job_es_table = create_es_publisher_sample_job(
    #     elasticsearch_index_alias='table_search_index',
    #     elasticsearch_doc_type_key='table',
    #     entity_type='table',
    #     model_name='databuilder.models.table_elasticsearch_document.TableESDocument')
    # job_es_table.launch()

    # job_es_user = create_es_publisher_sample_job(
    #     elasticsearch_index_alias='user_search_index',
    #     elasticsearch_doc_type_key='user',
    #     model_name='databuilder.models.user_elasticsearch_document.UserESDocument',
    #     entity_type='user',
    #     elasticsearch_mapping=USER_INDEX_MAP)
    # job_es_user.launch()

    # job_es_dashboard = create_es_publisher_sample_job(
    #     elasticsearch_index_alias='dashboard_search_index',
    #     elasticsearch_doc_type_key='dashboard',
    #     model_name='databuilder.models.dashboard_elasticsearch_document.DashboardESDocument',
    #     entity_type='dashboard',
    #     elasticsearch_mapping=DASHBOARD_ELASTICSEARCH_INDEX_MAPPING)
    # job_es_dashboard.launch()

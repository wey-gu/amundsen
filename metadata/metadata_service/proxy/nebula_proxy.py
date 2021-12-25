# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import textwrap
import re
import time
from random import randint
from typing import (Any, Dict, Iterable, List, Optional, Tuple,  # noqa: F401
                    Union, no_type_check)

import Nebula
import neobolt
from amundsen_common.entity.resource_type import ResourceType, to_resource_type
from amundsen_common.models.api import health_check
from amundsen_common.models.dashboard import DashboardSummary
from amundsen_common.models.feature import Feature, FeatureWatermark
from amundsen_common.models.generation_code import GenerationCode
from amundsen_common.models.lineage import Lineage, LineageItem
from amundsen_common.models.popular_table import PopularTable
from amundsen_common.models.table import (Application, Badge, Column,
                                          ProgrammaticDescription, Reader,
                                          ResourceReport, Source, SqlJoin,
                                          SqlWhere, Stat, Table, TableSummary,
                                          Tag, User, Watermark)
from amundsen_common.models.user import User as UserEntity
from amundsen_common.models.user import UserSchema
from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options
from flask import current_app, has_app_context
from nebula2.common.ttypes import ErrorCode
from nebula2.Config import Config
from nebula2.data.ResultSet import ResultSet
from nebula2.gclient.net import ConnectionPool, Session

from metadata_service import config
from metadata_service.entity.dashboard_detail import \
    DashboardDetail as DashboardDetailEntity
from metadata_service.entity.dashboard_query import \
    DashboardQuery as DashboardQueryEntity
from metadata_service.entity.description import Description
from metadata_service.entity.tag_detail import TagDetail
from metadata_service.exception import NotFoundException
from metadata_service.proxy.base_proxy import BaseProxy
from metadata_service.proxy.statsd_utilities import timer_with_counter
from metadata_service.util import UserResourceRel

_CACHE = CacheManager(**parse_cache_config_options({'cache.type': 'memory'}))

# Expire cache every 11 hours + jitter
_GET_POPULAR_RESOURCES_CACHE_EXPIRY_SEC = 11 * 60 * 60 + randint(0, 3600)

CREATED_EPOCH_MS = 'publisher_created_epoch_ms'
LAST_UPDATED_EPOCH_MS = 'publisher_last_updated_epoch_ms'
PUBLISHED_PROPERTY_NAME = 'published'

LOGGER = logging.getLogger(__name__)


class NebulaProxy(BaseProxy):
    """
    A proxy to Nebula (Gateway to Nebula)
    """

    def __init__(self, *,
                 host: str,
                 port: int,
                 user: str = 'Nebula',
                 password: str = '',
                 num_conns: int = 50,
                 max_connection_lifetime_sec: int = 100,
                 encrypted: bool = False,
                 validate_ssl: bool = False,
                 **kwargs: dict) -> None:
        """
        There's currently no request timeout from client side where server
        side can be enforced via "dbms.transaction.timeout"
        By default, it will set max number of connections to 50 and connection time out to 10 seconds.
        :param endpoint: Nebula endpoint
        :param num_conns: number of connections
        :param max_connection_lifetime_sec: max life time the connection can have when it comes to reuse. In other
        words, connection life time longer than this value won't be reused and closed on garbage collection. This
        value needs to be smaller than surrounding network environment's timeout.
        """
        endpoint = f'{host}:{port}'
        LOGGER.info('Nebula endpoint: {}'.format(endpoint))
        trust = Nebula.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES if validate_ssl else Nebula.TRUST_ALL_CERTIFICATES
        self._driver = GraphDatabase.driver(endpoint, max_connection_pool_size=num_conns,
                                            connection_timeout=10,
                                            max_connection_lifetime=max_connection_lifetime_sec,
                                            auth=(user, password),
                                            encrypted=encrypted,
                                            trust=trust)  # type: Driver

    def health(self) -> health_check.HealthCheck:
        """
        Runs one or more series of checks on the service. Can also
        optionally return additional metadata about each check (e.g.
        latency to database, cpu utilization, etc.).
        """
        checks = {}
        try:
            # dbms.cluster.overview() is only available for enterprise Nebula users
            cluster_overview = self._execute_query(statement='CALL dbms.cluster.overview()', param_dict={})
            checks = dict(cluster_overview.single())
            checks['overview_enabled'] = True
            status = health_check.OK
        except neobolt.exceptions.ClientError:
            checks = {'overview_enabled': False}
            status = health_check.OK  # Can connect to database but plugin is not available
        except Exception:
            status = health_check.FAIL
        final_checks = {f'{type(self).__name__}:connection': checks}
        return health_check.HealthCheck(status=status, checks=final_checks)

    @timer_with_counter
    def get_table(self, *, table_uri: str) -> Table:
        """
        :param table_uri: Table URI
        :return:  A Table object
        """

        cols, last_Nebula_record = self._exec_col_query(table_uri)

        readers = self._exec_usage_query(table_uri)

        wmk_results, table_writer, table_apps, timestamp_value, owners, tags, source, \
            badges, prog_descs, resource_reports = self._exec_table_query(table_uri)

        # TBD
        # joins, filters = self._exec_table_query_query(table_uri)

        table = Table(database=last_Nebula_record['db']['name'],
                      cluster=last_Nebula_record['clstr']['name'],
                      schema=last_Nebula_record['schema']['name'],
                      name=last_Nebula_record['tbl']['name'],
                      tags=tags,
                      badges=badges,
                      description=self._safe_get(last_Nebula_record, 'tbl_dscrpt', 'description'),
                      columns=cols,
                      owners=owners,
                      table_readers=readers,
                      watermarks=wmk_results,
                      table_writer=table_writer,
                      table_apps=table_apps,
                      last_updated_timestamp=timestamp_value,
                      source=source,
                      is_view=self._safe_get(last_Nebula_record, 'tbl', 'is_view'),
                      programmatic_descriptions=prog_descs,
                      resource_reports=resource_reports
                      )

        return table

    @timer_with_counter
    def _exec_col_query(self, table_uri: str) -> Tuple:
        # Return Value: (Columns, Last Processed Record)

        column_level_query = Template("""
        MATCH (db:Database)-[:CLUSTER]->(clstr:Cluster)-[:SCHEMA]->(schema:Schema)
        -[:TABLE]->(tbl:Table)-[:COLUMN]->(col:Column)
        WHERE id(tbl) == "{ vid }"
        RETURN db, clstr, schema, tbl, col, col.sort_order as sort_order
        ORDER BY sort_order;""")

        tbl_col_Nebula_records = self._execute_query(
            statement=column_level_query, param_dict={'vid': table_uri})
        cols = []
        last_Nebula_record = None
        for tbl_col_Nebula_record in tbl_col_Nebula_records:
            # Getting last record from this for loop as Nebula's result's random access is O(n) operation.
            col_stats = []
            for stat in tbl_col_Nebula_record['col_stats']:
                col_stat = Stat(
                    stat_type=stat['stat_type'],
                    stat_val=stat['stat_val'],
                    start_epoch=int(float(stat['start_epoch'])),
                    end_epoch=int(float(stat['end_epoch']))
                )
                col_stats.append(col_stat)

            column_badges = self._make_badges(tbl_col_Nebula_record['col_badges'])

            last_Nebula_record = tbl_col_Nebula_record
            col = Column(name=tbl_col_Nebula_record['col']['name'],
                         description=self._safe_get(tbl_col_Nebula_record, 'col_dscrpt', 'description'),
                         col_type=tbl_col_Nebula_record['col']['col_type'],
                         sort_order=int(tbl_col_Nebula_record['col']['sort_order']),
                         stats=col_stats,
                         badges=column_badges)

            cols.append(col)

        if not cols:
            raise NotFoundException('Table URI( {table_uri} ) does not exist'.format(table_uri=table_uri))

        return sorted(cols, key=lambda item: item.sort_order), last_Nebula_record

    @timer_with_counter
    def _exec_usage_query(self, table_uri: str) -> List[Reader]:
        # Return Value: List[Reader]

        usage_query = Template("""
        MATCH (`user`:`User`)-[read:READ]->(table:Table)
        WHERE id(table)=="{ vid }"
        RETURN user.email as email, read.read_count as read_count, table.name as table_name
        ORDER BY read_count DESC LIMIT 5;
        """)

        usage_nebula_records = self._execute_query(statement=usage_query,
                                                         param_dict={'vid': table_uri})
        readers = []  # type: List[Reader]
        for usage_Nebula_record in usage_Nebula_records:
            reader_data = self._get_user_details(user_id=usage_Nebula_record['email'])
            reader = Reader(user=self._build_user_from_record(record=reader_data),
                            read_count=usage_Nebula_record['read_count'])
            readers.append(reader)

        return readers

    @timer_with_counter
    def _exec_table_query(self, table_uri: str) -> Tuple:
        """
        Queries one Cypher record with watermark list, Application,
        ,timestamp, owner records and tag records.
        """

        # Return Value: (Watermark Results, Table Writer, Last Updated Timestamp, owner records, tag records)

        # TBD: {tag_type: default} removed for now, it requires index in builder.publisher side.
        table_level_query = Template("""
        MATCH (tbl:Table)
        WHERE id(tbl) == "{{ table_uri }}"
        OPTIONAL MATCH (wmk:Watermark)-[:BELONG_TO_TABLE]->(tbl)
        OPTIONAL MATCH (app_producer:Application)-[:GENERATES]->(tbl)
        OPTIONAL MATCH (app_consumer:Application)-[:CONSUMES]->(tbl)
        OPTIONAL MATCH (tbl)-[:LAST_UPDATED_AT]->(t:`Timestamp`)
        OPTIONAL MATCH (owner:`User`)<-[:OWNER]-(tbl)
        OPTIONAL MATCH (tbl)-[:TAGGED_BY]->(`tag`:`Tag)
        OPTIONAL MATCH (tbl)-[:HAS_BADGE]->(badge:Badge)
        OPTIONAL MATCH (tbl)-[:SOURCE]->(src:Source)
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(prog_descriptions:Programmatic_Description)
        OPTIONAL MATCH (tbl)-[:HAS_REPORT]->(resource_reports:Report)
        RETURN collect(distinct wmk) as wmk_records,
        collect(distinct app_producer) as producing_apps,
        collect(distinct app_consumer) as consuming_apps,
        t.last_updated_timestamp as last_updated_timestamp,
        collect(distinct owner) as owner_records,
        collect(distinct `tag`) as tag_records,
        collect(distinct badge) as badge_records,
        src,
        collect(distinct prog_descriptions) as prog_descriptions,
        collect(distinct resource_reports) as resource_reports
        """)

        table_records = self._execute_query(
            table_level_query.render(
                table_uri=table_uri
                )

        table_records = table_records.single()

        wmk_results = []
        wmk_records = table_records['wmk_records']
        for record in wmk_records:
            if record['key'] is not None:
                watermark_type = record['key'].split('/')[-2]
                wmk_result = Watermark(watermark_type=watermark_type,
                                       partition_key=record['partition_key'],
                                       partition_value=record['partition_value'],
                                       create_time=record['create_time'])
                wmk_results.append(wmk_result)

        tags = []
        if table_records.get('tag_records'):
            tag_records = table_records['tag_records']
            for record in tag_records:
                tag_result = Tag(tag_name=record['key'],
                                 tag_type=record['tag_type'])
                tags.append(tag_result)

        # this is for any badges added with BadgeAPI instead of TagAPI
        badges = self._make_badges(table_records.get('badge_records'))

        table_writer, table_apps = self._create_apps(table_records['producing_apps'], table_records['consuming_apps'])

        timestamp_value = table_records['last_updated_timestamp']

        owner_record = []

        for owner in table_records.get('owner_records', []):
            owner_data = self._get_user_details(user_id=owner['email'])
            owner_record.append(self._build_user_from_record(record=owner_data))

        src = None

        if table_records['src']:
            src = Source(source_type=table_records['src']['source_type'],
                         source=table_records['src']['source'])

        prog_descriptions = self._extract_programmatic_descriptions_from_query(
            table_records.get('prog_descriptions', [])
        )

        resource_reports = self._extract_resource_reports_from_query(table_records.get('resource_reports', []))

        return wmk_results, table_writer, table_apps, timestamp_value, owner_record,\
            tags, src, badges, prog_descriptions, resource_reports

    @timer_with_counter
    def _exec_table_query_query(self, table_uri: str) -> Tuple:
        """
        Queries one Cypher record with results that contain information about queries
        and entities (e.g. joins, where clauses, etc.) associated to queries that are executed
        on the table.
        """
        # TBD
        # - depending on coalesce https://github.com/vesoft-inc/nebula/issues/3522
        # - sample data need to be added

        # Return Value: (Watermark Results, Table Writer, Last Updated Timestamp, owner records, tag records)
        table_query_level_query = Template("""
        MATCH (tbl:Table)
        WHERE id(tbl) == "{{ table_uri }}"
        OPTIONAL MATCH (tbl)-[:COLUMN]->(col:Column)-[COLUMN_JOINS_WITH]->(j:Join)
        OPTIONAL MATCH (j)-[JOIN_OF_COLUMN]->(col2:Column)
        OPTIONAL MATCH (j)-[JOIN_OF_QUERY]->(jq:Query)-[:HAS_EXECUTION]->(exec:Execution)
        WITH tbl, j, col, col2,
            sum(coalesce(exec.execution_count, 0)) as join_exec_cnt
        ORDER BY join_exec_cnt desc
        LIMIT 5
        WITH tbl,
            COLLECT(DISTINCT {
            join: {
                joined_on_table: {
                    database: case when j.left_table_key == "{{ table_uri }}"
                              then j.right_database
                              else j.left_database
                              end,
                    cluster: case when j.left_table_key == "{{ table_uri }}"
                             then j.right_cluster
                             else j.left_cluster
                             end,
                    schema: case when j.left_table_key == "{{ table_uri }}"
                            then j.right_schema
                            else j.left_schema
                            end,
                    name: case when j.left_table_key == "{{ table_uri }}"
                          then j.right_table
                          else j.left_table
                          end
                },
                joined_on_column: col2.name,
                column: col.name,
                join_type: j.join_type,
                join_sql: j.join_sql
            },
            join_exec_cnt: join_exec_cnt
        }) as joins
        WITH tbl, joins
        OPTIONAL MATCH (tbl)-[:COLUMN]->(col:Column)-[USES_WHERE_CLAUSE]->(whr:Where)
        OPTIONAL MATCH (whr)-[WHERE_CLAUSE_OF]->(wq:Query)-[:HAS_EXECUTION]->(whrexec:Execution)
        WITH tbl, joins,
            whr, sum(coalesce(whrexec.execution_count, 0)) as where_exec_cnt
        ORDER BY where_exec_cnt desc
        LIMIT 5
        RETURN tbl, joins,
          COLLECT(DISTINCT {
            where_clause: whr.where_clause,
            where_exec_cnt: where_exec_cnt
          }) as filters
        """)

        query_records = self._execute_query(query=table_query_level_query, param_dict={'tbl_key': table_uri})

        table_query_records = query_records.single()

        joins = self._extract_joins_from_query(table_query_records.get('joins', [{}]))
        filters = self._extract_filters_from_query(table_query_records.get('filters', [{}]))

        return joins, filters

    def _extract_programmatic_descriptions_from_query(self, raw_prog_descriptions: dict) -> list:
        prog_descriptions = []
        for prog_description in raw_prog_descriptions:
            source = prog_description['description_source']
            if source is None:
                LOGGER.error("A programmatic description with no source was found... skipping.")
            else:
                prog_descriptions.append(ProgrammaticDescription(source=source, text=prog_description['description']))
        prog_descriptions.sort(key=lambda x: x.source)
        return prog_descriptions

    def _extract_resource_reports_from_query(self, raw_resource_reports: dict) -> list:
        resource_reports = []
        for resource_report in raw_resource_reports:
            name = resource_report.get('name')
            if name is None:
                LOGGER.error("A report with no name found... skipping.")
            else:
                resource_reports.append(ResourceReport(name=name, url=resource_report['url']))

        resource_reports.sort(key=lambda x: x.name)
        return resource_reports

    def _extract_joins_from_query(self, joins: List[Dict]) -> List[Dict]:
        valid_joins = []
        for join in joins:
            join_data = join['join']
            if all(join_data.values()):
                new_sql_join = SqlJoin(join_sql=join_data['join_sql'],
                                       join_type=join_data['join_type'],
                                       joined_on_column=join_data['joined_on_column'],
                                       joined_on_table=TableSummary(**join_data['joined_on_table']),
                                       column=join_data['column'])
                valid_joins.append(new_sql_join)
        return valid_joins

    def _extract_filters_from_query(self, filters: List[Dict]) -> List[Dict]:
        return_filters = []
        for filt in filters:
            filter_where = filt.get('where_clause')
            if filter_where:
                return_filters.append(SqlWhere(where_clause=filter_where))
        return return_filters

    @no_type_check
    def _safe_get(self, dct, *keys):
        """
        Helper method for getting value from nested dict. This also works either key does not exist or value is None.
        :param dct:
        :param keys:
        :return:
        """
        for key in keys:
            dct = dct.get(key)
            if dct is None:
                return None
        return dct

    @timer_with_counter
    def _execute_query(self, query: str, session: Session) -> List:
        """
        :param query:
        :param session:
        :return:
        """
        try:
            r = session.execute_json(query)
            r_dict = json.loads(r)
            r_code = r_dict.get('code', -1)
            if r_code == 0:
                return r_dict.get('results')
            else:
                if LOGGER.isEnabledFor(logging.DEBUG):
                    LOGGER.debug(
                        "Failed executing query: %s, result %s", query, r)
                raise Exception
        except Exception as e:
            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug('Failed executing query: %s, except: %s', query, str(e))


    # noinspection PyMethodMayBeStatic
    def _make_badges(self, badges: Iterable) -> List[Badge]:
        """
        Generates a list of Badges objects

        :param badges: A list of badges of a table or a column
        :return: a list of Badge objects
        """
        _badges = []
        for badge in badges:
            _badges.append(Badge(badge_name=badge["key"], category=badge["category"]))
        return _badges

    @timer_with_counter
    def get_resource_description(self, *,
                                 tag: ResourceType,
                                 uri: str) -> Description:
        """
        Get the resource description based on the uri. Any exception will propagate back to api server.

        :param resource_type:
        :param uri:
        :return:
        """

        description_query = Template("""
        MATCH (n:`{{ tag }}`)-[:DESCRIPTION]->(d:Description)
        WHERE id(n) == "{{ uri }}"
        RETURN d.description AS description;
        """

        result = self._execute_query(description_query.render(
            tag=tag,
            uri=uri
            ))

        result = result.single()
        return Description(description=result['description'] if result else None)

    @timer_with_counter
    def get_table_description(self, *,
                              table_uri: str) -> Union[str, None]:
        """
        Get the table description based on table uri. Any exception will propagate back to api server.

        :param table_uri:
        :return:
        """

        return self.get_resource_description(tag=ResourceType.Table, uri=table_uri).description

    @timer_with_counter
    def put_resource_description(self, *,
                                 tag: ResourceType,
                                 uri: str,
                                 description: str) -> None:
        """
        Update resource description with one from user
        # TBD, to see if the edges should be added
        :param uri: Resource uri (key in Nebula)
        :param description: new value for resource description
        """
        desc_vid = uri + '/_description'

        description = re.escape(description)
        upsert_desc_query = Template("""
        UPDATE VERTEX ON `Description` "{{ desc_vid }}"
        SET `description` = "{{ description }}"
        WHEN description != "{{ description }}"
        YIELD description;
        """)

        start = time.time()

        try:
            # TBD on finished it
            result = self._execute_query(upsert_desc_query.render(
                description=description,
                desc_vid=desc_vid
                ))

            if not result.single():
                raise NotFoundException(f'Failed to update the description as resource {uri} does not exist')

        except Exception as e:
            LOGGER.exception('Failed to execute update process')

            # propagate exception back to api
            raise e

        finally:
            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug('Update process elapsed for {} seconds'.format(time.time() - start))

    @timer_with_counter
    def put_table_description(self, *,
                              table_uri: str,
                              description: str) -> None:
        """
        Update table description with one from user
        :param table_uri: Table uri (key in Nebula)
        :param description: new value for table description
        """

        self.put_resource_description(resource_type=ResourceType.Table,
                                      uri=table_uri,
                                      description=description)

    @timer_with_counter
    def get_column_description(self, *,
                               table_uri: str,
                               column_name: str) -> Union[str, None]:
        """
        Get the column description based on table uri. Any exception will propagate back to api server.

        :param table_uri:
        :param column_name:
        :return:
        """
        column_vid = f"{ table_uri }/{ column_name }"
        column_description_query = Template("""
            MATCH (tbl:Table)-[:COLUMN]->(c:Column)-[:DESCRIPTION]->(d:Description)
            WHERE id(tbl) == "{{ table_uri }}" AND id(c) == "{{ column_vid }}"
            RETURN d.description AS description;
        """)

        result = self._execute_query(query=column_description_query.render(
            table_uri=table_uri,
            column_vid=column_vid))

        column_descrpt = result.single()

        column_description = column_descrpt['description'] if column_descrpt else None

        return column_description

    @timer_with_counter
    def put_column_description(self, *,
                               table_uri: str,
                               column_name: str,
                               description: str) -> None:
        """
        Update column description with input from user
        :param table_uri:
        :param column_name:
        :param description:
        :return:
        """

        column_uri = f"{ table_uri }/{ column_name }"

        self.put_resource_description(resource_type=ResourceType.Column,
                                      uri=column_uri,
                                      description=description)

    @timer_with_counter
    def add_owner(self, *,
                  table_uri: str,
                  owner: str) -> None:
        """
        Update table owner informations.
        1. Do a create if not exists query of the owner(`user`) node.
        2. Do a upsert of the owner/owned_by relation.
        :param table_uri:
        :param owner:
        :return:
        """

        self.add_resource_owner(uri=table_uri,
                                resource_type=ResourceType.Table,
                                owner=owner)

    @timer_with_counter
    def add_resource_owner(self, *,
                           uri: str,
                           resource_type: ResourceType,
                           owner: str) -> None:
        """
        Update table owner informations.
        1. Do a create if not exists query of the owner(`user`) node.
        2. Do a upsert of the owner/owned_by relation.

        :param table_uri:
        :param owner:
        :return:
        """
        user_email = owner
        create_owner_query = Template("""
            UPSERT VERTEX ON `User` "{{ user_email }}"
            SET email = "{{ user_email }}"
            WHEN email != "{{ user_email }}"
            YIELD email;

            UPSERT EDGE ON OWNER
            "{{ user_email }}" -> "{{ uri }}"
            SET START_LABEL = "User",
                END_LABEL = "{{ resource_type }}"
            WHEN START_LABEL != "User";

            UPSERT EDGE ON OWNED_BY
            "{{ uri }}" -> "{{ user_email }}"
            SET END_LABEL = "User",
                START_LABEL = "{{ resource_type }}"
            WHEN END_LABEL != "User";
        """)

        try:
            # TBD on handle result
            result = self._execute_query(create_owner_query.render(
                user_email=user_email,
                uri=uri,
                resource_type=resource_type
                ))

            if not result.single():
                raise RuntimeError('Failed to create relation between '
                                   'owner {owner} and resource {uri}'.format(owner=owner,
                                                                             uri=uri))
            tx.commit()
        except Exception as e:
            if not tx.closed():
                tx.rollback()
            # propagate the exception back to api
            raise e

    @timer_with_counter
    def delete_owner(self, *,
                     table_uri: str,
                     owner: str) -> None:
        """
        Delete the owner / owned_by relationship.
        :param table_uri:
        :param owner:
        :return:
        """
        self.delete_resource_owner(uri=table_uri,
                                   resource_type=ResourceType.Table,
                                   owner=owner)

    @timer_with_counter
    def delete_resource_owner(self, *,
                              uri: str,
                              resource_type: ResourceType,
                              owner: str) -> None:
        """
        Delete the owner / owned_by relationship.
        :param table_uri:
        :param owner:
        :return:
        """
        delete_query = Template("""
            DELETE EDGE OWNER_OF "{{ owner }}" -> "{{ uri }}";
            DELETE EDGE OWNED_BY "{{ uri }}" -> "{{ owner }}";
        """)
        try:
            # TBD on handle result
            result = self._execute_query(create_owner_query.render(
                owner=owner,
                uri=uri
                ))
        except Exception as e:
            # propagate the exception back to api
            if not tx.closed():
                tx.rollback()
            raise e
        finally:
            tx.commit()

    @timer_with_counter
    def add_badge(self, *,
                  id: str,
                  badge_name: str,
                  category: str = '',
                  resource_type: ResourceType) -> None:

        LOGGER.info('New badge {} for id {} with category {} '
                    'and resource type {}'.format(badge_name, id, category, resource_type.name))

        validation_query = \
            'MATCH (n:{resource_type} {{key: $key}}) return n'.format(resource_type=resource_type.name)

        upsert_badge_query = Template("""
        MERGE (u:Badge {key: $badge_name})
        on CREATE SET u={key: $badge_name, category: $category}
        on MATCH SET u={key: $badge_name, category: $category}
        """)

        upsert_badge_relation_query = Template("""
        MATCH(n1:Badge {{key: $badge_name, category: $category}}),
        (n2:{resource_type} {{key: $key}})
        MERGE (n1)-[r1:BADGE_FOR]->(n2)-[r2:HAS_BADGE]->(n1)
        RETURN id(n1), id(n2)
        """.format(resource_type=resource_type.name))

        try:
            tx = self._driver.session().begin_transaction()
            tbl_result = tx.run(validation_query, {'key': id})
            if not tbl_result.single():
                raise NotFoundException('id {} does not exist'.format(id))

            tx.run(upsert_badge_query, {'badge_name': badge_name,
                                        'category': category})

            result = tx.run(upsert_badge_relation_query, {'badge_name': badge_name,
                                                          'key': id,
                                                          'category': category})

            if not result.single():
                raise RuntimeError('failed to create relation between '
                                   'badge {badge} and resource {resource} of resource type '
                                   '{resource_type} MORE {q}'.format(badge=badge_name,
                                                                     resource=id,
                                                                     resource_type=resource_type,
                                                                     q=upsert_badge_relation_query))
            tx.commit()
        except Exception as e:
            if not tx.closed():
                tx.rollback()
            raise e

    @timer_with_counter
    def delete_badge(self, id: str,
                     badge_name: str,
                     category: str,
                     resource_type: ResourceType) -> None:

        # TODO for some reason when deleting it will say it was successful
        # even when the badge never existed to begin with
        LOGGER.info('Delete badge {} for id {} with category {}'.format(badge_name, id, category))

        # only deletes relationshop between badge and resource
        delete_query = Template("""
        MATCH (b:Badge {{key:$badge_name, category:$category}})-
        [r1:BADGE_FOR]->(n:{resource_type} {{key: $key}})-[r2:HAS_BADGE]->(b) DELETE r1,r2
        """.format(resource_type=resource_type.name))

        try:
            tx = self._driver.session().begin_transaction()
            tx.run(delete_query, {'badge_name': badge_name,
                                  'key': id,
                                  'category': category})
            tx.commit()
        except Exception as e:
            # propagate the exception back to api
            if not tx.closed():
                tx.rollback()
            raise e

    @timer_with_counter
    def get_badges(self) -> List:
        LOGGER.info('Get all badges')
        query = Template("""
        MATCH (b:Badge) RETURN b as badge
        """)
        records = self._execute_query(statement=query,
                                             param_dict={})
        results = []
        for record in records:
            results.append(Badge(badge_name=record['badge']['key'],
                                 category=record['badge']['category']))

        return results

    @timer_with_counter
    def add_tag(self, *,
                id: str,
                tag: str,
                tag_type: str = 'default',
                resource_type: ResourceType = ResourceType.Table) -> None:
        """
        Add new tag
        1. Create the node with type Tag if the node doesn't exist.
        2. Create the relation between tag and table if the relation doesn't exist.

        :param id:
        :param tag:
        :param tag_type:
        :param resource_type:
        :return: None
        """
        LOGGER.info('New tag {} for id {} with type {} and resource type {}'.format(tag, id, tag_type,
                                                                                    resource_type.name))

        validation_query = \
            'MATCH (n:{resource_type} {{key: $key}}) return n'.format(resource_type=resource_type.name)

        upsert_tag_query = Template("""
        MERGE (u:Tag {key: $tag})
        on CREATE SET u={tag_type: $tag_type, key: $tag}
        on MATCH SET u={tag_type: $tag_type, key: $tag}
        """)

        upsert_tag_relation_query = Template("""
        MATCH (n1:Tag {{key: $tag, tag_type: $tag_type}}), (n2:{resource_type} {{key: $key}})
        MERGE (n1)-[r1:TAG]->(n2)-[r2:TAGGED_BY]->(n1)
        RETURN id(n1), id(n2)
        """.format(resource_type=resource_type.name))

        try:
            tx = self._driver.session().begin_transaction()
            tbl_result = tx.run(validation_query, {'key': id})
            if not tbl_result.single():
                raise NotFoundException('id {} does not exist'.format(id))

            # upsert the node
            tx.run(upsert_tag_query, {'tag': tag,
                                      'tag_type': tag_type})
            result = tx.run(upsert_tag_relation_query, {'tag': tag,
                                                        'key': id,
                                                        'tag_type': tag_type})
            if not result.single():
                raise RuntimeError('Failed to create relation between '
                                   'tag {tag} and resource {resource} of resource type: {resource_type}'
                                   .format(tag=tag,
                                           resource=id,
                                           resource_type=resource_type.name))
            tx.commit()
        except Exception as e:
            if not tx.closed():
                tx.rollback()
            # propagate the exception back to api
            raise e

    @timer_with_counter
    def delete_tag(self, *,
                   id: str,
                   tag: str,
                   tag_type: str = 'default',
                   resource_type: ResourceType = ResourceType.Table) -> None:
        """
        Deletes tag
        1. Delete the relation between resource and the tag
        2. todo(Tao): need to think about whether we should delete the tag if it is an orphan tag.

        :param id:
        :param tag:
        :param tag_type: {default-> normal tag, badge->non writable tag from UI}
        :param resource_type:
        :return:
        """

        LOGGER.info('Delete tag {} for id {} with type {} and resource type: {}'.format(tag, id,
                                                                                        tag_type, resource_type.name))
        delete_query = Template("""
        MATCH (n1:Tag{{key: $tag, tag_type: $tag_type}})-
        [r1:TAG]->(n2:{resource_type} {{key: $key}})-[r2:TAGGED_BY]->(n1) DELETE r1,r2
        """.format(resource_type=resource_type.name))

        try:
            tx = self._driver.session().begin_transaction()
            tx.run(delete_query, {'tag': tag,
                                  'key': id,
                                  'tag_type': tag_type})
            tx.commit()
        except Exception as e:
            # propagate the exception back to api
            if not tx.closed():
                tx.rollback()
            raise e

    @timer_with_counter
    def get_tags(self) -> List:
        """
        Get all existing tags from Nebula

        :return:
        """
        LOGGER.info('Get all the tags')
        # todo: Currently all the tags are default type, we could open it up if we want to include badge
        query = Template("""
        MATCH (t:Tag{tag_type: 'default'})
        OPTIONAL MATCH (resource)-[:TAGGED_BY]->(t)
        WITH t as tag_name, count(distinct id(resource)) as tag_count
        WHERE tag_count > 0
        RETURN tag_name, tag_count
        """)

        records = self._execute_query(statement=query,
                                             param_dict={})
        results = []
        for record in records:
            results.append(TagDetail(tag_name=record['tag_name']['key'],
                                     tag_count=record['tag_count']))
        return results

    @timer_with_counter
    def get_latest_updated_ts(self) -> Optional[int]:
        """
        API method to fetch last updated / index timestamp for Nebula, es

        :return:
        """
        query = Template("""
        MATCH (n:Updatedtimestamp{key: 'amundsen_updated_timestamp'}) RETURN n as ts
        """)
        record = self._execute_query(statement=query,
                                            param_dict={})
        # None means we don't have record for Nebula, es last updated / index ts
        record = record.single()
        if record:
            return record.get('ts', {}).get('latest_timestamp', 0)
        else:
            return None

    @timer_with_counter
    def get_statistics(self) -> Dict[str, Any]:
        # Not implemented
        # TBD, use stats job or index, to be decided
        pass

    @_CACHE.cache('_get_global_popular_resources_uris', expire=_GET_POPULAR_RESOURCES_CACHE_EXPIRY_SEC)
    def _get_global_popular_resources_uris(self, num_entries: int,
                                           resource_type: ResourceType = ResourceType.Table) -> List[str]:
        """
        Retrieve popular table uris. Will provide tables with top x popularity score.
        Popularity score = number of distinct readers * log(total number of reads)
        The result of this method will be cached based on the key (num_entries), and the cache will be expired based on
        _GET_POPULAR_TABLE_CACHE_EXPIRY_SEC

        For score computation, it uses logarithm on total number of reads so that score won't be affected by small
        number of users reading a lot of times.
        :return: Iterable of table uri
        """
        # NOTE, this means we need TAG INDEX for all.
        query = Template("""
        MATCH (resource:`{{ resource_type }}`)-[r:READ_BY]->(u:`User`)
        WITH id(resource) as resource_key, count(distinct u) as readers, sum(r.read_count) as total_reads
        WHERE readers >= {{ num_readers }}
        RETURN resource_key, readers, total_reads, (readers * log(total_reads)) as score
        ORDER BY score DESC LIMIT {{ num_entries }};
        """).format(resource_type=resource_type.name)
        LOGGER.info('Querying popular tables URIs')
        num_readers = current_app.config['POPULAR_RESOURCES_MINIMUM_READER_COUNT']
        # TBD need to handle results
        records = self._execute_query(query=query.render(
            resource_type=resource_type,
            num_readers=num_readers,
            num_entries=num_entries
            ))

        return [record['resource_key'] for record in records]

    @timer_with_counter
    @_CACHE.cache('_get_personal_popular_tables_uris', _GET_POPULAR_RESOURCES_CACHE_EXPIRY_SEC)
    def _get_personal_popular_resources_uris(self, num_entries: int,
                                             user_id: str,
                                             resource_type: ResourceType = ResourceType.Table) -> List[str]:
        """
        Retrieve personalized popular resources uris. Will provide resources with top
        popularity score that have been read by a peer of the user_id provided.
        The popularity score is defined in the same way as `_get_global_popular_resources_uris`

        The result of this method will be cached based on the key (num_entries, user_id),
        and the cache will be expired based on _GET_POPULAR_TABLE_CACHE_EXPIRY_SEC

        :return: Iterable of table uri
        """
        query = Template("""
        MATCH (u:`User`)<-[:READ_BY]-(:`{{ resource_type }}`)-[:READ_BY]->
             (coUser:`User`)<-[coRead:READ_BY]-(resource:`{{ resource_type }}`)
        WHERE id(u) == "{{ user_id }}"
        WITH id(resource) AS resource_key, count(DISTINCT coUser) AS co_readers,
             sum(coRead.read_count) AS total_co_reads
        WHERE co_readers >= {{ num_readers }}
        RETURN resource_key, (co_readers * log(total_co_reads)) AS score
        ORDER BY score DESC LIMIT {{ num_entries }};
        """).format(resource_type=resource_type.name)
        LOGGER.info('Querying popular tables URIs')
        num_readers = current_app.config['POPULAR_RESOURCES_MINIMUM_READER_COUNT']
        # TBD need to handle results
        records = self._execute_query(query=query.render(
            resource_type=resource_type,
            user_id=user_id,
            num_entries=num_entries
            ))

        return [record['resource_key'] for record in records]

    @timer_with_counter
    def get_popular_tables(self, *,
                           num_entries: int,
                           user_id: Optional[str] = None) -> List[PopularTable]:
        """

        Retrieve popular tables. As popular table computation requires full scan of table and user relationship,
        it will utilize cached method _get_popular_tables_uris.

        :param num_entries:
        :return: Iterable of PopularTable
        """
        if user_id is None:
            # Get global popular table URIs
            table_uris = self._get_global_popular_resources_uris(num_entries)
        else:
            # Get personalized popular table URIs
            table_uris = self._get_personal_popular_resources_uris(num_entries, user_id)

        if not table_uris:
            return []

        # TBD, render table_uris into list in string
        # TBD, check results

        query = Template("""
        MATCH (db:Database)-[:CLUSTER]->(clstr:Cluster)-[:SCHEMA]->(schema:Schema)-[:TABLE]->(tbl:Table)
        WHERE id(tbl) IN {{ table_uris }}
        WITH db.name as database_name, clstr.name as cluster_name, schema.name as schema_name, tbl
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(dscrpt:Description)
        RETURN database_name, cluster_name, schema_name, tbl.name as table_name,
        dscrpt.description as table_description;
        """)

        # TBD, handle result
        records = self._execute_query(query=query.render(table_uris=table_uris))

        popular_tables = []
        for record in records:
            popular_table = PopularTable(database=record['database_name'],
                                         cluster=record['cluster_name'],
                                         schema=record['schema_name'],
                                         name=record['table_name'],
                                         description=self._safe_get(record, 'table_description'))
            popular_tables.append(popular_table)
        return popular_tables

    def _get_popular_tables(self, *, resource_uris: List[str]) -> List[TableSummary]:
        """

        """
        if not resource_uris:
            return []
        # TBD render resource_uris into string

        query = Template("""
        MATCH (db:Database)-[:CLUSTER]->(clstr:Cluster)-[:SCHEMA]->(schema:Schema)-[:TABLE]->(tbl:Table)
        WHERE id(tbl) IN {{ table_uris }}
        WITH db.name as database_name, clstr.name as cluster_name, schema.name as schema_name, tbl
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(dscrpt:Description)
        RETURN database_name, cluster_name, schema_name, tbl.name as table_name,
        dscrpt.description as table_description;
        """)
        records = self._execute_query(query=query.render(table_uris=resource_uris))

        popular_tables = []
        for record in records:
            popular_table = TableSummary(database=record['database_name'],
                                         cluster=record['cluster_name'],
                                         schema=record['schema_name'],
                                         name=record['table_name'],
                                         description=self._safe_get(record, 'table_description'))
            popular_tables.append(popular_table)
        return popular_tables

    def _get_popular_dashboards(self, *, resource_uris: List[str]) -> List[DashboardSummary]:
        """

        """
        if not resource_uris:
            return []
        # TBD render resource_uris into string

        query = Template("""
        MATCH (d:Dashboard)-[:DASHBOARD_OF]->(dg:Dashboardgroup)-[:DASHBOARD_GROUP_OF]->(c:Cluster)
        WHERE id(d) IN {{ dashboards_uris }}
        OPTIONAL MATCH (d)-[:DESCRIPTION]->(dscrpt:Description)
        OPTIONAL MATCH (d)-[:EXECUTED]->(last_exec:Execution)
        WHERE split(id(last_exec), '/')[5] == '_last_successful_execution'
        RETURN c.name as cluster_name, dg.name as dg_name, dg.dashboard_group_url as dg_url,
        id(d) as uri, d.name as name, d.dashboard_url as url,
        split(id(d), '_')[0] as product,
        dscrpt.description as description, last_exec.timestamp as last_successful_run_timestamp
        """)

        records = self._execute_query(statement=query,
                                             param_dict={'dashboards_uris': resource_uris})

        popular_dashboards = []
        for record in records:
            popular_dashboards.append(DashboardSummary(
                uri=record['uri'],
                cluster=record['cluster_name'],
                group_name=record['dg_name'],
                group_url=record['dg_url'],
                product=record['product'],
                name=record['name'],
                url=record['url'],
                description=record['description'],
                last_successful_run_timestamp=record['last_successful_run_timestamp'],
            ))

        return popular_dashboards

    @timer_with_counter
    def get_popular_resources(self, *,
                              num_entries: int,
                              resource_types: List[str],
                              user_id: Optional[str] = None) -> Dict[str, List]:
        popular_resources: Dict[str, List] = dict()
        for resource in resource_types:
            resource_type = to_resource_type(label=resource)
            popular_resources[resource_type.name] = list()
            if user_id is None:
                # Get global popular Table/Dashboard URIs
                resource_uris = self._get_global_popular_resources_uris(num_entries,
                                                                        resource_type=resource_type)
            else:
                # Get personalized popular Table/Dashboard URIs
                resource_uris = self._get_personal_popular_resources_uris(num_entries,
                                                                          user_id,
                                                                          resource_type=resource_type)

            if resource_type == ResourceType.Table:
                popular_resources[resource_type.name] = self._get_popular_tables(
                    resource_uris=resource_uris
                )
            elif resource_type == ResourceType.Dashboard:
                popular_resources[resource_type.name] = self._get_popular_dashboards(
                    resource_uris=resource_uris
                )

        return popular_resources

    @timer_with_counter
    def get_user(self, *, id: str) -> Union[UserEntity, None]:
        """
        Retrieve user detail based on `user`_id(email).

        :param id: the email for the given user
        :return:
        """

        query = Template("""
        MATCH (`user`:`User`)
        WHERE id(`user`) == "{{ id }}"
        OPTIONAL MATCH (`user`)-[:MANAGE_BY]->(manager:`User`)
        RETURN user as user_record, manager as manager_record
        """)
        # TBD need to handle result
        record = self._execute_query(query=query.render(id=id))
        single_result = record.single()

        if not single_result:
            raise NotFoundException('User {user_id} '
                                    'not found in the graph'.format(user_id=id))

        record = single_result.get('user_record', {})
        manager_record = single_result.get('manager_record', {})
        if manager_record:
            manager_name = manager_record.get('full_name', '')
        else:
            manager_name = ''

        return self._build_user_from_record(record=record, manager_name=manager_name)

    def create_update_user(self, *, user: User) -> Tuple[User, bool]:
        """
        Create a user if it does not exist, otherwise update the user. Required
        fields for creating / updating a user are validated upstream to this when
        the User object is created.

        :param user:
        :return:
        """
        user_data = UserSchema().dump(user)
        properties, values = self._create_props_pair(user_data)

        create_update_user_query = Template("""
            INSERT VERTEX `User` ({{ properties }}) VALUES "{{ user_id }}":({{ values }})
        """)

        try:
            # TBD: to handle later
            # - CREATED_EPOCH_MS
            # - results
            records = self._execute_query(query.render(
                properties=properties,
                values =values,
                user_id=user.user_id
                ))

            user_result = result.single()
            if not user_result:
                raise RuntimeError('Failed to create user with data %s' % user_data)
            tx.commit()

            new_user = self._build_user_from_record(user_result['usr'])
            new_user_created = True if user_result['created'] is True else False

        except Exception as e:
            if not tx.closed():
                tx.rollback()
            # propagate the exception back to api
            raise e

        return new_user, new_user_created

    def _create_props_pair(self,
                           record_dict: dict) -> Tuple[str, str]:
        """
        Creates a Nebula DML properties and values
        """

        properties, values = [], []
        for k, v in record_dict.items():
            if v or v is False:
                properties.append(f"`{ k }`")
                if type(v) is str:
                    values.append(f'"{ v }"')
                else:
                    values.append(str(v))

        properties.append(PUBLISHED_PROPERTY_NAME)
        values.append(f'"api_create_update_user"')

        properties.append(LAST_UPDATED_EPOCH_MS)
        values.append('timestamp()')

        return ', '.join(properties), ', '.join(values)

    def get_users(self) -> List[UserEntity]:
        # TBD create USER index on is_active
        query = "MATCH (usr:`User`) WHERE usr.is_active == true RETURN collect(usr) as users"
        # TBD handle results
        record = self._execute_query(query=query)
        result = record.single()
        if not result or not result.get('users'):
            raise NotFoundException('Error getting users')

        return [self._build_user_from_record(record=rec) for rec in result['users']]

    @staticmethod
    def _build_user_from_record(record: dict, manager_name: Optional[str] = None) -> UserEntity:
        """
        Builds user record from Cypher query result. Other than the one defined in amundsen_common.models.user.User,
        you could add more fields from User node into the User model by specifying keys in config.USER_OTHER_KEYS
        :param record:
        :param manager_name:
        :return:
        """
        other_key_values = {}
        if has_app_context() and current_app.config[config.USER_OTHER_KEYS]:
            for k in current_app.config[config.USER_OTHER_KEYS]:
                if k in record:
                    other_key_values[k] = record[k]

        return UserEntity(email=record['email'],
                          user_id=record.get('user_id', record['email']),
                          first_name=record.get('first_name'),
                          last_name=record.get('last_name'),
                          full_name=record.get('full_name'),
                          is_active=record.get('is_active', True),
                          profile_url=record.get('profile_url'),
                          github_username=record.get('github_username'),
                          team_name=record.get('team_name'),
                          slack_id=record.get('slack_id'),
                          employee_type=record.get('employee_type'),
                          role_name=record.get('role_name'),
                          manager_fullname=record.get('manager_fullname', manager_name),
                          other_key_values=other_key_values)

    @staticmethod
    def _get_user_resource_relationship_clause(
            relation_type: UserResourceRel, id: str = None,
            user_id: str = None,
            resource_type: ResourceType = ResourceType.Table) -> Tuple[str, str]:
        """
        Returns the relationship clause and the where clause of a cypher query between users and tables
        The User node is 'usr', the table node is 'tbl', and the relationship is 'rel'
        e.g. (usr:`User`)-[rel:READ]->(tbl:Table), (usr)-[rel:READ]->(tbl)
        """
        resource_matcher: str = ''
        user_matcher: str = ''
        where_clause: str = ''

        if id is not None:
            resource_matcher += ':{}'.format(resource_type.name)
            if id != '':
                where_clause += f'WHERE id(resource) == "{ id }" '

        if user_id is not None:
            user_matcher += ':User'
            if user_id != '':
                if where_clause.startswith('WHERE'):
                    where_clause += 'AND '
                else:
                    where_clause += 'WHERE '

                where_clause += f'id(usr) == "{ user_id }" '

        if relation_type == UserResourceRel.follow:
            relation = f'(resource{resource_matcher})-[r1:FOLLOWED_BY]->(usr{user_matcher})-[r2:FOLLOW]->' \
                       f'(resource{resource_matcher})'
        elif relation_type == UserResourceRel.own:
            relation = f'(resource{resource_matcher})-[r1:OWNER]->(usr{user_matcher})-[r2:OWNER_OF]->' \
                       f'(resource{resource_matcher})'
        elif relation_type == UserResourceRel.read:
            relation = f'(resource{resource_matcher})-[r1:READ_BY]->(usr{user_matcher})-[r2:READ]->' \
                       f'(resource{resource_matcher})'
        else:
            raise NotImplementedError(f'The relation type {relation_type} is not defined!')
        return relation, where_clause

    @staticmethod
    def _get_user_resource_edge_type(
            relation_type: UserResourceRel) -> Tuple[str, str]:

        if relation_type == UserResourceRel.follow:
            edge_type = "FOLLOW"
            reverse_edge_type = "FOLLOWED_BY"
        elif relation_type == UserResourceRel.own:
            edge_type = "OWNER_OF"
            reverse_edge_type = "OWNER"
        elif relation_type == UserResourceRel.read:
            edge_type = "READ"
            reverse_edge_type = "READ_BY"
        else:
            raise NotImplementedError(f'The relation type {relation_type} is not defined!')
        return edge_type, reverse_edge_type

    @timer_with_counter
    def get_dashboard_by_user_relation(self, *, user_email: str, relation_type: UserResourceRel) \
            -> Dict[str, List[DashboardSummary]]:
        """
        Retrieve all follow the Dashboard per user based on the relation.

        :param user_email: the email of the user
        :param relation_type: the relation between the user and the resource
        :return:
        """
        rel_clause: str, where_clause: str = self._get_user_resource_relationship_clause(
            relation_type=relation_type,
            id='',
            resource_type=ResourceType.Dashboard,
            user_id=user_email)

        # FYI, to extract last_successful_execution, it searches for its execution ID which is always
        # _last_successful_execution
        # https://github.com/amundsen-io/amundsendatabuilder/blob/master/databuilder/models/dashboard/dashboard_execution.py#L18
        # https://github.com/amundsen-io/amundsendatabuilder/blob/master/databuilder/models/dashboard/dashboard_execution.py#L24

        query = Template("""
        MATCH {{ rel_clause }}<-[:DASHBOARD]-(dg:Dashboardgroup)<-[:DASHBOARD_GROUP]-(clstr:Cluster)
        {{ where_clause }}
        OPTIONAL MATCH (resource)-[:DESCRIPTION]->(dscrpt:Description)
        OPTIONAL MATCH (resource)-[:EXECUTED]->(last_exec:Execution)
        WHERE split(id(last_exec), '/')[5] == '_last_successful_execution'
        RETURN clstr.name as cluster_name, dg.name as dg_name, dg.dashboard_group_url as dg_url,
        id(resource) as uri, resource.name as name, resource.dashboard_url as url,
        split(id(resource), '_')[0] as product,
        dscrpt.description as description, last_exec.timestamp as last_successful_run_timestamp""")

        # TBD handle the result
        records = self._execute_query(query=query.render(
            rel_cluase=rel_clause,
            where_clause=where_clause))

        if not records:
            raise NotFoundException('User {user_id} does not {relation} on {resource_type} resources'.format(
                user_id=user_email,
                relation=relation_type,
                resource_type=ResourceType.Dashboard.name))

        results = []
        for record in records:
            results.append(DashboardSummary(
                uri=record['uri'],
                cluster=record['cluster_name'],
                group_name=record['dg_name'],
                group_url=record['dg_url'],
                product=record['product'],
                name=record['name'],
                url=record['url'],
                description=record['description'],
                last_successful_run_timestamp=record['last_successful_run_timestamp'],
            ))

        return {ResourceType.Dashboard.name.lower(): results}

    @timer_with_counter
    def get_table_by_user_relation(self, *, user_email: str, relation_type: UserResourceRel) \
            -> Dict[str, List[PopularTable]]:
        """
        Retrive all follow the Table per user based on the relation.

        :param user_email: the email of the user
        :param relation_type: the relation between the user and the resource
        :return:
        """
        rel_clause: str, where_clause: str = self._get_user_resource_relationship_clause(
            relation_type=relation_type,
            id='',
            resource_type=ResourceType.Table,
            user_id=user_email)

        query = Template("""
            MATCH {{ rel_clause }}<-[:TABLE]-(schema:Schema)<-[:SCHEMA]-(clstr:Cluster)<-[:CLUSTER]-(db:Database)
            {{ where_clause }}
            WITH db, clstr, schema, resource
            OPTIONAL MATCH (resource)-[:DESCRIPTION]->(tbl_dscrpt:Description)
            RETURN db, clstr, schema, resource, tbl_dscrpt""")

        # TBD handle the result
        records = self._execute_query(query=query.render(
            rel_cluase=rel_clause,
            where_clause=where_clause))

        if not table_records:
            raise NotFoundException('User {user_id} does not {relation} any resources'.format(user_id=user_email,
                                                                                              relation=relation_type))
        results = []
        for record in table_records:
            results.append(PopularTable(
                database=record['db']['name'],
                cluster=record['clstr']['name'],
                schema=record['schema']['name'],
                name=record['resource']['name'],
                description=self._safe_get(record, 'tbl_dscrpt', 'description')))
        return {ResourceType.Table.name.lower(): results}

    @timer_with_counter
    def get_frequently_used_tables(self, *, user_email: str) -> Dict[str, Any]:
        """
        Retrieves all Table the resources per user on READ relation.

        :param user_email: the email of the user
        :return:
        """

        query = Template("""
        MATCH (u:`User`)-[r:READ]->(tbl:Table)
        WHERE id(u) == "{{ user_email }}" AND EXISTS(r.published) AND r.published IS NOT NULL
        WITH u, r, tbl ORDER BY r.published DESC, r.read_count DESC LIMIT 50
        MATCH (tbl:Table)<-[:TABLE]-(schema:Schema)<-[:SCHEMA]-(clstr:Cluster)<-[:CLUSTER]-(db:Database)
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(tbl_dscrpt:Description)
        RETURN db, clstr, schema, tbl, tbl_dscrpt
        """)

        # TBD need to handle results
        table_records = self._execute_query(
            query=query.render(user_email=user_email))

        if not table_records:
            raise NotFoundException('User {user_id} does not READ any resources'.format(user_id=user_email))
        results = []

        for record in table_records:
            results.append(PopularTable(
                database=record['db']['name'],
                cluster=record['clstr']['name'],
                schema=record['schema']['name'],
                name=record['tbl']['name'],
                description=self._safe_get(record, 'tbl_dscrpt', 'description')))
        return {'table': results}

    @timer_with_counter
    def add_resource_relation_by_user(self, *,
                                      id: str,
                                      user_id: str,
                                      relation_type: UserResourceRel,
                                      resource_type: ResourceType) -> None:
        """
        Update table user informations.
        1. Do a upsert of the user node.
        2. Do a upsert of the relation/reverse-relation edge.

        :param table_uri:
        :param user_id:
        :param relation_type:
        :return:
        """
        edge_type, reverse_edge_type = self._get_user_resource_edge_type(
            relation_type=relation_type)

        query = Template("""

        UPSERT VERTEX ON `User` "{{ user_email }}"
        SET email = "{{ user_email }}"
        WHEN email != "{{ user_email }}"
        YIELD email;

        UPSERT VERTEX ON `ResourceType` "{{ resource_id }}"
        SET name = "{{ user_email }}"
        WHEN name != "{{ user_email }}"
        YIELD name;

        UPSERT EDGE ON `{{ edge_type }}`
        "{{ user_email }}" -> "{{ resource_id }}"
        SET START_LABEL = "User",
            END_LABEL = "{{ resource_type }}"
        WHEN START_LABEL != "User";

        UPSERT EDGE ON `{{ reverse_edge_type }}`
        "{{ resource_id }}" -> "{{ user_email }}"
        SET END_LABEL = "User",
            START_LABEL = "{{ resource_type }}"
        WHEN END_LABEL != "User";
        """)

        try:
            # TBD handle result
            result = self._execute_query(query=query.render(
                user_email=user_id,
                resource_id=id,
                resource_type=resource_type,
                edge_type=edge_type,
                reverse_edge_type=reverse_edge_type
                ))

            if not result.single():
                raise RuntimeError('Failed to create relation between '
                                   'user {user} and resource {id}'.format(user=user_id,
                                                                          id=id))
            tx.commit()
        except Exception as e:
            if not tx.closed():
                tx.rollback()
            # propagate the exception back to api
            raise e

    @timer_with_counter
    def delete_resource_relation_by_user(self, *,
                                         id: str,
                                         user_id: str,
                                         relation_type: UserResourceRel,
                                         resource_type: ResourceType) -> None:
        """
        Delete the relationship between user and resources.

        :param table_uri:
        :param user_id:
        :param relation_type:
        :return:
        """
        edge_type, reverse_edge_type = self._get_user_resource_edge_type(
            relation_type=relation_type)

        delete_query = Template("""
            DELETE EDGE `{{ edge_type }}` "{{ owner }}" -> "{{ uri }}";
            DELETE EDGE `{{ reverse_edge_type }}` "{{ uri }}" -> "{{ owner }}";
            """)

        try:
            # TBD handle result
            result = self._execute_query(create_owner_query.render(
                owner=owner,
                uri=uri
                ))
        except Exception as e:
            # propagate the exception back to api
            if not tx.closed():
                tx.rollback()
            raise e

    @timer_with_counter
    def get_dashboard(self,
                      id: str,
                      ) -> DashboardDetailEntity:
        # TBD tag_type: default removed(this introduced index)
        # TBD need to verify results
        get_dashboard_detail_query = Template("""
        MATCH (d:Dashboard )-[:DASHBOARD_OF]->(dg:Dashboardgroup)-[:DASHBOARD_GROUP_OF]->(c:Cluster)
        WHERE id(d) == "{{ id }}"
        OPTIONAL MATCH (d)-[:DESCRIPTION]->(description:Description)
        OPTIONAL MATCH (d)-[:EXECUTED]->(last_exec:Execution) WHERE split(id(last_exec), '/')[5] == '_last_execution'
        OPTIONAL MATCH (d)-[:EXECUTED]->(last_success_exec:Execution)
        WHERE split(id(last_success_exec), '/')[5] == '_last_successful_execution'
        OPTIONAL MATCH (d)-[:LAST_UPDATED_AT]->(t:`Timestamp`)
        OPTIONAL MATCH (d)-[:OWNER]->(owner:`User`)
        WITH c, dg, d, description, last_exec, last_success_exec, t, collect(owner) as owners
        OPTIONAL MATCH (d)-[:TAGGED_BY]->(`tag`:`Tag`)
        OPTIONAL MATCH (d)-[:HAS_BADGE]->(badge:Badge)
        WITH c, dg, d, description, last_exec, last_success_exec, t, owners, collect(`tag`) as `tags`,
        collect(badge) as badges
        OPTIONAL MATCH (d)-[read:READ_BY]->(:`User`)
        WITH c, dg, d, description, last_exec, last_success_exec, t, owners, `tags`, badges,
        sum(read.read_count) as recent_view_count
        OPTIONAL MATCH (d)-[:HAS_QUERY]->(`query`:`Query`)
        WITH c, dg, d, description, last_exec, last_success_exec, t, owners, `tags`, badges,
        recent_view_count, collect({name: `query`.name, url: `query`.url, query_text: `query`.query_text}) as queries
        OPTIONAL MATCH (d)-[:DASHBOARD_WITH_TABLE]->(table:Table)<-[:TABLE]-(schema:Schema)
        <-[:SCHEMA]-(cluster:Cluster)<-[:CLUSTER]-(db:Database)
        OPTIONAL MATCH (table)-[:DESCRIPTION]->(table_description:Description)
        WITH c, dg, d, description, last_exec, last_success_exec, t, owners, `tags`, badges,
        recent_view_count, queries,
        collect({name: table.name, schema: schema.name, cluster: cluster.name, database: db.name,
        description: table_description.description}) as tables
        RETURN
        c.name as cluster_name,
        id(d) as uri,
        d.dashboard_url as url,
        d.name as name,
        split(id(d), '_')[0] as product,
        toInteger(d.created_timestamp) as created_timestamp,
        description.description as description,
        dg.name as group_name,
        dg.dashboard_group_url as group_url,
        toInteger(last_success_exec.`timestamp`) as last_successful_run_timestamp,
        toInteger(last_exec.`timestamp`) as last_run_timestamp,
        last_exec.state as last_run_state,
        toInteger(t.`timestamp`) as updated_timestamp,
        owners,
        `tags`,
        badges,
        recent_view_count,
        queries,
        tables;
        """)

        # TBD to handle result
        dashboard_record = self._execute_query(query=get_dashboard_detail_query.render(
            id=id
            ))

        if not dashboard_record:
            raise NotFoundException('No dashboard exist with URI: {}'.format(id))

        owners = []

        for owner in dashboard_record.get('owners', []):
            owner_data = self._get_user_details(user_id=owner['email'], user_data=owner)
            owners.append(self._build_user_from_record(record=owner_data))

        tags = [Tag(tag_type=tag['tag_type'], tag_name=tag['key']) for tag in dashboard_record['tags']]

        badges = self._make_badges(dashboard_record['badges'])

        chart_names = [chart['name'] for chart in dashboard_record['charts'] if 'name' in chart and chart['name']]
        # TODO Deprecate query_names in favor of queries after several releases from v2.5.0
        query_names = [query['name'] for query in dashboard_record['queries'] if 'name' in query and query['name']]
        queries = [DashboardQueryEntity(**query) for query in dashboard_record['queries']
                   if query.get('name') or query.get('url') or query.get('text')]
        tables = [PopularTable(**table) for table in dashboard_record['tables'] if 'name' in table and table['name']]

        return DashboardDetailEntity(uri=dashboard_record['uri'],
                                     cluster=dashboard_record['cluster_name'],
                                     url=dashboard_record['url'],
                                     name=dashboard_record['name'],
                                     product=dashboard_record['product'],
                                     created_timestamp=dashboard_record['created_timestamp'],
                                     description=self._safe_get(dashboard_record, 'description'),
                                     group_name=self._safe_get(dashboard_record, 'group_name'),
                                     group_url=self._safe_get(dashboard_record, 'group_url'),
                                     last_successful_run_timestamp=self._safe_get(dashboard_record,
                                                                                  'last_successful_run_timestamp'),
                                     last_run_timestamp=self._safe_get(dashboard_record, 'last_run_timestamp'),
                                     last_run_state=self._safe_get(dashboard_record, 'last_run_state'),
                                     updated_timestamp=self._safe_get(dashboard_record, 'updated_timestamp'),
                                     owners=owners,
                                     tags=tags,
                                     badges=badges,
                                     recent_view_count=dashboard_record['recent_view_count'],
                                     chart_names=chart_names,
                                     query_names=query_names,
                                     queries=queries,
                                     tables=tables
                                     )

    @timer_with_counter
    def get_dashboard_description(self, *,
                                  id: str) -> Description:
        """
        Get the dashboard description based on dashboard uri. Any exception will propagate back to api server.

        :param id:
        :return:
        """

        return self.get_resource_description(resource_type=ResourceType.Dashboard, uri=id)


    @timer_with_counter
    def get_resources_using_table(self, *,
                                  id: str,
                                  resource_type: ResourceType) -> Dict[str, List[DashboardSummary]]:
        """

        :param id:
        :param resource_type:
        :return:
        """
        if resource_type != ResourceType.Dashboard:
            raise NotImplementedError('{} is not supported'.format(resource_type))

        get_dashboards_using_table_query = Template("""
        MATCH (d:Dashboard)-[:DASHBOARD_WITH_TABLE]->(table:Table),
        (d)-[:DASHBOARD_OF]->(dg:Dashboardgroup)-[:DASHBOARD_GROUP_OF]->(c:Cluster)
        WHERE id(table) == "{{ id }}"
        OPTIONAL MATCH (d)-[:DESCRIPTION]->(description:Description)
        OPTIONAL MATCH (d)-[:EXECUTED]->(last_success_exec:Execution)
        WHERE split(id(last_success_exec), '/')[5] == '_last_successful_execution'
        OPTIONAL MATCH (d)-[read:READ_BY]->(:`User`)
        WITH c, dg, d, description, last_success_exec, sum(read.read_count) as recent_view_count
        RETURN
        id(d) as uri,
        c.name as cluster,
        dg.name as group_name,
        dg.dashboard_group_url as group_url,
        d.name as name,
        d.dashboard_url as url,
        description.description as description,
        split(id(d), '_')[0] as product,
        toInteger(last_success_exec.`timestamp`) as last_successful_run_timestamp, recent_view_count
        ORDER BY recent_view_count DESC;
        """)

        records = self._execute_query(query=get_dashboards_using_table_query(
            id=id))

        results = []

        for record in records:
            results.append(DashboardSummary(**record))
        return {'dashboards': results}

    @timer_with_counter
    def get_lineage(self, *,
                    id: str, resource_type: ResourceType, direction: str, depth: int = 1) -> Lineage:
        """
        Retrieves the lineage information for the specified resource type.

        :param id: key of a table or a column
        :param resource_type: Type of the entity for which lineage is being retrieved
        :param direction: Whether to get the upstream/downstream or both directions
        :param depth: depth or level of lineage information
        :return: The Lineage object with upstream & downstream lineage items
        """

        # TBD, there could be issues on the result, need to verify when multi match was finished.
        get_both_lineage_query = Template("""
        MATCH (source:`{{ resource }}`)
        WHERE id(source) == "{{ id }}"
        OPTIONAL MATCH dpath=(source)-[downstream_len:HAS_DOWNSTREAM*..{{ depth }}]->(downstream_entity:`{{ resource }}`)
        OPTIONAL MATCH upath=(source)-[upstream_len:HAS_UPSTREAM*..{{ depth }}]->(upstream_entity:`{{ resource }}`)
        WITH downstream_entity, upstream_entity, downstream_len, upstream_len, upath, dpath
        OPTIONAL MATCH (upstream_entity)-[:HAS_BADGE]->(upstream_badge:Badge)
        OPTIONAL MATCH (downstream_entity)-[:HAS_BADGE]->(downstream_badge:Badge)
        WITH CASE WHEN downstream_badge IS NULL THEN collect(NULL)
        ELSE collect(distinct {key:id(downstream_badge),category:downstream_badge.category})
        END AS downstream_badges, CASE WHEN upstream_badge IS NULL THEN collect(NULL)
        ELSE collect(distinct {key:id(upstream_badge),category:upstream_badge.category})
        END AS upstream_badges, upstream_entity, downstream_entity, upstream_len, downstream_len, upath, dpath
        OPTIONAL MATCH (downstream_entity:`{{ resource }}`)-[downstream_read:READ_BY]->(:`User`)
        WITH upstream_entity, downstream_entity, upstream_len, downstream_len, upath, dpath,
        downstream_badges, upstream_badges, sum(downstream_read.read_count) AS downstream_read_count
        OPTIONAL MATCH (upstream_entity:`{{ resource }}`)-[upstream_read:READ_BY]->(:`User`)
        WITH upstream_entity, downstream_entity, upstream_len, downstream_len,
        downstream_badges, upstream_badges, downstream_read_count,
        sum(upstream_read.read_count) AS upstream_read_count, upath, dpath
        WITH CASE WHEN upstream_len IS NULL THEN collect(NULL)
        ELSE COLLECT(distinct{level:SIZE(upstream_len), source:split(id(upstream_entity),'://')[0],
        key:id(upstream_entity), badges:upstream_badges, usage:upstream_read_count, parent:id(nodes(upath)[-2])})
        END AS upstream_entities, CASE WHEN downstream_len IS NULL THEN collect(NULL)
        ELSE COLLECT(distinct{level:SIZE(downstream_len), source:split(id(downstream_entity),'://')[0],
        key:id(downstream_entity), badges:downstream_badges, usage:downstream_read_count, parent:id(nodes(dpath)[-2])})
        END AS downstream_entities RETURN downstream_entities, upstream_entities
        """)

        get_upstream_lineage_query = Template("""
        MATCH (source:`{{ resource }}`)
        WHERE id(source) == "{{ id }}"
        OPTIONAL MATCH path=(source)-[upstream_len:HAS_UPSTREAM*..{{ depth }}]->(upstream_entity:`{{ resource }}`)
        WITH upstream_entity, upstream_len, path
        OPTIONAL MATCH (upstream_entity)-[:HAS_BADGE]->(upstream_badge:Badge)
        WITH CASE WHEN upstream_badge IS NULL THEN collect(NULL)
        ELSE collect(distinct {key:id(upstream_badge),category:upstream_badge.category})
        END AS upstream_badges, upstream_entity, upstream_len, path
        OPTIONAL MATCH (upstream_entity:`{{ resource }}`)-[upstream_read:READ_BY]->(:`User`)
        WITH upstream_entity, upstream_len, upstream_badges,
        sum(upstream_read.read_count) AS upstream_read_count, path
        WITH CASE WHEN upstream_len IS NULL THEN collect(NULL)
        ELSE COLLECT(distinct{level:SIZE(upstream_len), source:split(id(upstream_entity),'://')[0],
        key:id(upstream_entity), badges:upstream_badges, usage:upstream_read_count, parent:id(nodes(path)[-2])})
        END AS upstream_entities RETURN upstream_entities
        """)

        get_downstream_lineage_query = Template("""
        MATCH (source:`{{ resource }}`)
        WHERE id(source) == "{{ id }}"
        OPTIONAL MATCH path=(source)-[downstream_len:HAS_DOWNSTREAM*..{{ depth }}]->(downstream_entity:`{{ resource }}`)
        WITH downstream_entity, downstream_len, path
        OPTIONAL MATCH (downstream_entity)-[:HAS_BADGE]->(downstream_badge:Badge)
        WITH CASE WHEN downstream_badge IS NULL THEN collect(NULL)
        ELSE collect(distinct {key:id(downstream_badge),category:downstream_badge.category})
        END AS downstream_badges, downstream_entity, downstream_len, path
        OPTIONAL MATCH (downstream_entity:`{{ resource }}`)-[downstream_read:READ_BY]->(:`User`)
        WITH downstream_entity, downstream_len, downstream_badges,
        sum(downstream_read.read_count) AS downstream_read_count, path
        WITH CASE WHEN downstream_len IS NULL THEN collect(NULL)
        ELSE COLLECT(distinct{level:SIZE(downstream_len), source:split(id(downstream_entity),'://')[0],
        key:id(downstream_entity), badges:downstream_badges, usage:downstream_read_count, parent:id(nodes(path)[-2])})
        END AS downstream_entities RETURN downstream_entities
        """)

        if direction == 'upstream':
            lineage_query = get_upstream_lineage_query

        elif direction == 'downstream':
            lineage_query = get_downstream_lineage_query

        else:
            lineage_query = get_both_lineage_query

        records = self._execute_query(query=lineage_query.render(
            depth=depth,
            resource=resource_type,
            id=id
            ))

        result = records.single()

        downstream_tables = []
        upstream_tables = []

        for downstream in result.get("downstream_entities") or []:
            downstream_tables.append(LineageItem(**{"key": downstream["key"],
                                                    "source": downstream["source"],
                                                    "level": downstream["level"],
                                                    "badges": self._make_badges(downstream["badges"]),
                                                    "usage": downstream.get("usage", 0),
                                                    "parent": downstream.get("parent", '')
                                                    }))

        for upstream in result.get("upstream_entities") or []:
            upstream_tables.append(LineageItem(**{"key": upstream["key"],
                                                  "source": upstream["source"],
                                                  "level": upstream["level"],
                                                  "badges": self._make_badges(upstream["badges"]),
                                                  "usage": upstream.get("usage", 0),
                                                  "parent": upstream.get("parent", '')
                                                  }))

        # ToDo: Add a root_entity as an item, which will make it easier for lineage graph
        return Lineage(**{"key": id,
                          "upstream_entities": upstream_tables,
                          "downstream_entities": downstream_tables,
                          "direction": direction, "depth": depth})

    def _create_watermarks(self, wmk_records: List) -> List[Watermark]:
        watermarks = []
        for record in wmk_records:
            if record['key'] is not None:
                watermark_type = record['key'].split('/')[-2]
                watermarks.append(Watermark(watermark_type=watermark_type,
                                            partition_key=record['partition_key'],
                                            partition_value=record['partition_value'],
                                            create_time=record['create_time']))
        return watermarks

    def _create_feature_watermarks(self, wmk_records: List) -> List[Watermark]:
        watermarks = []
        for record in wmk_records:
            if record['key'] is not None:
                watermark_type = record['key'].split('/')[-1]

                watermarks.append(FeatureWatermark(key=record['key'],
                                                   watermark_type=watermark_type,
                                                   time=record['time']))
        return watermarks

    def _create_programmatic_descriptions(self, prog_desc_records: List) -> List[ProgrammaticDescription]:
        programmatic_descriptions = []
        for pg in prog_desc_records:
            source = pg['description_source']
            if source is None:
                LOGGER.error("A programmatic description with no source was found... skipping.")
            else:
                programmatic_descriptions.append(ProgrammaticDescription(source=source,
                                                                         text=pg['description']))
        return programmatic_descriptions

    def _create_owners(self, owner_records: List) -> List[User]:
        owners = []
        for owner in owner_records:
            owners.append(User(email=owner['email']))
        return owners

    def _create_app(self, app_record: dict, kind: str) -> Application:
        return Application(
            name=app_record['name'],
            id=app_record['id'],
            application_url=app_record['application_url'],
            description=app_record.get('description'),
            kind=kind,
        )

    def _create_apps(self,
                     producing_app_records: List,
                     consuming_app_records: List) -> Tuple[Application, List[Application]]:

        table_apps = []
        for record in producing_app_records:
            table_apps.append(self._create_app(record, kind='Producing'))

        # for bw compatibility, we populate table_writer with one of the producing apps
        table_writer = table_apps[0] if table_apps else None

        _producing_app_ids = {app.id for app in table_apps}
        for record in consuming_app_records:
            # if an app has both a consuming and producing relationship with a table
            # (e.g. an app that reads writes back to its input table), we call it a Producing app and
            # do not add it again
            if record['id'] not in _producing_app_ids:
                table_apps.append(self._create_app(record, kind='Consuming'))

        return table_writer, table_apps

    @timer_with_counter
    def _exec_feature_query(self, *, feature_key: str) -> Dict:
        """
        Executes cypher query to get feature and related nodes
        """
        # TBD introducing test data

        feature_query = Template("""
        MATCH (feat:Feature)
        WHERE id(feat) == "{{ feature_key }}"
        OPTIONAL MATCH (db:Database)-[:AVAILABLE_FEATURE]->(feat)
        OPTIONAL MATCH (fg:Feature_Group)-[:GROUPS]->(feat)
        OPTIONAL MATCH (feat)-[:OWNER]->(owner:`User`)
        OPTIONAL MATCH (feat)-[:TAGGED_BY]->(tag:Tag)
        OPTIONAL MATCH (feat)-[:HAS_BADGE]->(badge:Badge)
        OPTIONAL MATCH (feat)-[:DESCRIPTION]->(desc:Description)
        OPTIONAL MATCH (feat)-[:DESCRIPTION]->(prog_descriptions:Programmatic_Description)
        OPTIONAL MATCH (wmk:Feature_Watermark)-[:BELONG_TO_FEATURE]->(feat)
        RETURN feat, desc, fg,
        collect(distinct wmk) as wmk_records,
        collect(distinct db) as availability_records,
        collect(distinct owner) as owner_records,
        collect(distinct tag) as tag_records,
        collect(distinct badge) as badge_records,
        collect(distinct prog_descriptions) as prog_descriptions
        """)

        # TBD handle results
        results = self._execute_query(query=feature_query(
            feature_key=feature_key))

        if results is None:
            raise NotFoundException('Feature with key {} does not exist'.format(feature_key))

        feature_records = results.single()
        if feature_records is None:
            raise NotFoundException('Feature with key {} does not exist'.format(feature_key))

        watermarks = self._create_feature_watermarks(wmk_records=feature_records['wmk_records'])

        availability_records = [db['name'] for db in feature_records.get('availability_records')]

        description = None
        if feature_records.get('desc'):
            description = feature_records.get('desc')['description']

        programmatic_descriptions = self._create_programmatic_descriptions(feature_records['prog_descriptions'])

        owners = self._create_owners(feature_records['owner_records'])

        tags = []
        for record in feature_records.get('tag_records'):
            tag_result = Tag(tag_name=record['key'],
                             tag_type=record['tag_type'])
            tags.append(tag_result)

        feature_node = feature_records['feat']

        feature_group = feature_records['fg']

        return {
            'key': feature_node.get('key'),
            'name': feature_node.get('name'),
            'version': feature_node.get('version'),
            'feature_group': feature_group.get('name'),
            'data_type': feature_node.get('data_type'),
            'entity': feature_node.get('entity'),
            'description': description,
            'programmatic_descriptions': programmatic_descriptions,
            'last_updated_timestamp': feature_node.get('last_updated_timestamp'),
            'created_timestamp': feature_node.get('created_timestamp'),
            'watermarks': watermarks,
            'availability': availability_records,
            'tags': tags,
            'badges': self._make_badges(feature_records.get('badge_records')),
            'owners': owners,
            'status': feature_node.get('status')
        }

    def get_feature(self, *, feature_uri: str) -> Feature:
        """
        :param feature_uri: uniquely identifying key for a feature node
        :return: a Feature object
        """
        feature_metadata = self._exec_feature_query(feature_key=feature_uri)

        feature = Feature(
            key=feature_metadata['key'],
            name=feature_metadata['name'],
            version=feature_metadata['version'],
            status=feature_metadata['status'],
            feature_group=feature_metadata['feature_group'],
            entity=feature_metadata['entity'],
            data_type=feature_metadata['data_type'],
            availability=feature_metadata['availability'],
            description=feature_metadata['description'],
            owners=feature_metadata['owners'],
            badges=feature_metadata['badges'],
            tags=feature_metadata['tags'],
            programmatic_descriptions=feature_metadata['programmatic_descriptions'],
            last_updated_timestamp=feature_metadata['last_updated_timestamp'],
            created_timestamp=feature_metadata['created_timestamp'],
            watermarks=feature_metadata['watermarks'])
        return feature

    def get_resource_generation_code(self, *, uri: str, resource_type: ResourceType) -> GenerationCode:
        """
        Executes cypher query to get query nodes associated with resource
        """

        query = Template("""
        MATCH (feat:{{ resource_type }})
        WHERE id(feat) == "{{ uri }}"
        OPTIONAL MATCH (q:Feature_Generation_Code)-[:GENERATION_CODE_OF]->(feat)
        RETURN q as query_records
        """
        # TBD handle results
        records = self._execute_query(query=query.render(
            resource_type=resource_type, uri=uri)))
        if records is None:
            raise NotFoundException('Generation code for id {} does not exist'.format(id))

        query_result = records.single()['query_records']
        if query_result is None:
            raise NotFoundException('Generation code for id {} does not exist'.format(id))

        return GenerationCode(key=query_result['key'],
                              text=query_result['text'],
                              source=query_result['source'])

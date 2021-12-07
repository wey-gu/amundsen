# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import csv
import ctypes
import json
import logging
import random
import time
from io import open
from os import listdir
from os.path import isfile, join
from typing import Callable, List, Set, TypeVar

import pandas
from jinja2 import Template
from nebula2.common.ttypes import ErrorCode
from nebula2.Config import Config
from nebula2.data.ResultSet import ResultSet
from nebula2.gclient.net import ConnectionPool, Session
from pyhocon import ConfigFactory, ConfigTree

from databuilder.publisher.base_publisher import Publisher


# Setting field_size_limit to solve the error below
# _csv.Error: field larger than field limit (131072)
# https://stackoverflow.com/a/54517228/5972935
csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))

# Config keys
# A directory that contains CSV files for nodes
VERTEX_FILES_DIR = "vertex_files_directory"
# A directory that contains CSV files for relationships
EDGE_FILES_DIR = "edge_files_directory"
# Endpoint list for Nebula e.g: 192.168.11.1:9669,192.168.11.2:9669
NEBULA_ENDPOINTS = "nebula_endpoints"
NEBULA_MAX_CONN_POOL_SIZE = "nebula_max_conn_pool_size"

NEBULA_USER = "nebula_user"
NEBULA_PASSWORD = "nebula_password"
NEBULA_SPACE = "nebula_space"
NEBULA_INSERT_BATCHSIZE = "nebula_insert_batchsize"
NEBULA_RETRY_NUMBER = "nebula_retry_number"

LAST_UPDATED_EPOCH_MS = 'publisher_last_updated_epoch_ms'
PUBLISHED_TAG_PROPERTY_NAME = 'published_tag'
PUBLISHED_EDGE_PROPERTY_NAME = 'published_edge'
JOB_PUBLISH_TAG = 'job_publish_tag'

# CSV HEADER

# A header for Nebula Tag
TAG_KEY = "LABEL"
# A header for Nebula Vertex ID
VID_KEY = "KEY"
# Required columns for Tag
TAG_REQUIRED_KEYS = {TAG_KEY, VID_KEY}

# Edges relates two vertecis together
# Start vertex tag
EDGE_START_TAG = "START_TAG"
# Start vertex key
EDGE_START_KEY = "START_KEY"
# End vertex tag
EDGE_END_TAG = "END_TAG"
# Vertex key
EDGE_END_KEY = "END_KEY"
# Type for edge (Start Vertex)->(End Vertex)
EDGE_TYPE = "TYPE"
# Type for reverse edge (End Vertex)->(Start Vertex)
EDGE_REVERSE_TYPE = "REVERSE_TYPE"
# Required columns for Edge
EDGE_REQUIRED_KEYS = {
    EDGE_START_TAG,
    EDGE_START_KEY,
    EDGE_END_TAG,
    EDGE_END_KEY,
    EDGE_TYPE,
    EDGE_REVERSE_TYPE,
}
NEBULA_EXISTED = "existed"
NEBULA_UNQUOTED_TYPES = {"int64", "float"}

DEFAULT_CONFIG = ConfigFactory.from_dict({
    NEBULA_SPACE: "amundsen",
    NEBULA_INSERT_BATCHSIZE: 64,
    NEBULA_RETRY_NUMBER: 4}
)

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


class NebulaCsvPublisher(Publisher):
    """
    A Publisher takes two folders for input and publishes to Nebula.
    One folder will contain CSV file(s) for Vertex where the other
    folder will contain CSV file(s) for Edge.

    FIXME: To add needed documentations here.
    """

    def __init__(self) -> None:
        super(NebulaCsvPublisher, self).__init__()

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(DEFAULT_CONFIG)

        self._count: int = 0
        self._vertex_files = self._list_files(conf, VERTEX_FILES_DIR)
        self._vertex_files_iter = iter(self._vertex_files)

        self._edge_files = self._list_files(conf, EDGE_FILES_DIR)
        self._edge_files_iter = iter(self._edge_files)

        self._nebula_space = conf.get_string(NEBULA_SPACE)
        self._nebula_max_conn_pool_size = conf.get_int(
            NEBULA_MAX_CONN_POOL_SIZE)
        self._nebula_endpoints = conf.get_list(NEBULA_ENDPOINTS)
        self._conn_pool = self._init_connection_pool()
        self._nebula_credential = (
            conf.get_string(NEBULA_USER),
            conf.get_string(NEBULA_PASSWORD),
        )
        self._nebula_insert_batchsize = conf.get_int(NEBULA_INSERT_BATCHSIZE)
        self._nebula_retry_number = conf.get_int(NEBULA_RETRY_NUMBER)

        self.tags: Set[str] = set()
        self.publish_tag: str = conf.get_string(JOB_PUBLISH_TAG)
        if not self.publish_tag:
            raise Exception(f"{ JOB_PUBLISH_TAG } should not be empty")

        LOGGER.info(
            "Publishing vertex csv files %s, and edge CSV files %s",
            self._vertex_files,
            self._edge_files,
        )

    def retry_with_backoff(
            self, fn: Callable[[], T], backoff_in_sec: int = 1) -> T:
        """
        An exponential backoff decorator
        :param fn: The function to be retried.
        :param backoff_in_sec: The time in second per each backoff step
        """
        retry_num = self._nebula_retry_number
        attempts = 0
        while True:
            try:
                return fn()
            except:  # noqa: E722
                if attempts == retry_num:
                    LOGGER.debug("%s attempts of retry exceeded.", retry_num)
                    raise
                else:
                    sleep = backoff_in_sec * 2 ** attempts + random.uniform(
                        0, 1)
                    LOGGER.debug(
                        "Retried for %s times, sleeping for %s seconds...",
                        attempts,
                        sleep,
                    )
                    time.sleep(sleep)
                    attempts += 1

    def _init_connection_pool(self) -> ConnectionPool:
        """
        :return: ConnectionPool
        """
        connection_pool = ConnectionPool()
        config = Config()
        config.max_connection_pool_size = self._nebula_max_conn_pool_size
        connection_pool.init(self._nebula_endpoints, config)
        return connection_pool

    def _list_files(self, conf: ConfigTree, path_key: str) -> List[str]:
        """
        List files from directory
        :param conf:
        :param path_key:
        :return: List of file paths
        """
        if path_key not in conf:
            return []

        path = conf.get_string(path_key)
        return [join(path, f) for f in listdir(path) if isfile(join(path, f))]

    def publish_impl(self) -> None:  # noqa: C901
        """
        :return:
        """

        start = time.time()

        # Nebula is designed to be schemaful, read more on its documentation:
        # https://docs.nebula-graph.io/2.6.1/2.quick-start/4.nebula-graph-crud
        with self._conn_pool.session_context(
                *self._nebula_credential) as session:
            session.execute(f"USE { self._nebula_space };")
            LOGGER.info(
                "Creating schema based on vertex and edge files: %s, %s",
                self._vertex_files,
                self._edge_files,
            )
            self._ensure_schema(session=session)

        LOGGER.info("Importing data in vertex files: %s", self._vertex_files)
        # FIXME: Introduce concurrent execution in the future.
        with self._conn_pool.session_context(
                *self._nebula_credential) as session:
            session.execute(f"USE { self._nebula_space };")
            try:
                while True:
                    try:
                        vertex_file = next(self._vertex_files_iter)
                        self._import_vertices(vertex_file, session=session)
                    except StopIteration:
                        break

                LOGGER.info(
                    "Importing data in edge files: %s", self._edge_files)
                while True:
                    try:
                        edge_file = next(self._edge_files_iter)
                        self._import_edges(edge_file, session=session)
                    except StopIteration:
                        break
                LOGGER.info(
                    "Successfully published. Elapsed: %i seconds",
                    time.time() - start)
            except Exception as e:
                LOGGER.exception("Failed to publish.")
                raise e

    def get_scope(self) -> str:
        return "publisher.nebula"

    def make_create_tag_command(self, vertex_record: dict) -> str:
        """
        To make a CREATE TAG command
        :param vertex_record:
        :return:
        """
        template = Template(
            """
            CREATE TAG {{ TAG_KEY }}({{ PROPERTIES }});
        """
        )
        properties = self._ddl_props_body(vertex_record, TAG_REQUIRED_KEYS)

        return template.render(
            TAG_KEY=vertex_record[TAG_KEY], PROPERTIES=properties)

    def make_alter_tag_command(
            self, vertex_record: dict, cur_props: dict) -> str:
        """
        To make a ALTER TAG command
        :param vertex_record:
        :return:
        """
        target = {}
        for k, v in vertex_record.items():
            if k in TAG_REQUIRED_KEYS:
                continue

            prop_type = k.split(":")[-1]
            prop_name = k.removesuffix(f":{ prop_type }")
            target[prop_name] = prop_type
        involved = list(set(target.items()) - set(cur_props.items()))
        changed = [el for el in involved if el[0] in cur_props]
        added = [el for el in involved if el[0] not in cur_props]
        outdated = list(set(cur_props.items()) - set(target.items()))
        deleted = [el for el in outdated if el[0] not in target]

        template = Template(
            """
            {%- if changed %}ALTER TAG {{ TAG_KEY }} CHANGE(
                {%- for el in changed %}{{ el[0] }} {{ el[1] }} NULL \
                    {{- ", " if not loop.last else "" }}
                {%- endfor %});
            {%- endif %}
            {%- if added %}ALTER TAG {{ TAG_KEY }} ADD(
                {%- for el in added %}{{ el[0] }} {{ el[1] }} NULL \
                    {{- ", " if not loop.last else "" }}
                {%- endfor %});
            {%- endif %}
            {%- if deleted %}ALTER TAG {{ TAG_KEY }} DROP(
                {%- for el in deleted %}{{ el[0] }} {{ el[1] }} NULL \
                    {{- ", " if not loop.last else "" }}
                {%- endfor %});
            {%- endif %}
        """
        )

        return template.render(
            TAG_KEY=vertex_record[TAG_KEY],
            changed=changed,
            added=added,
            deleted=deleted,
        )

    def make_create_edgetype_command(self, edge_record: dict) -> str:
        """
        To make a CREATE EDGE command
        :param edge_record:
        :return:
        """
        template = Template(
            """
            CREATE EDGE {{ EDGE_TYPE }}({{ PROPERTIES }});
        """
        )
        properties = self._ddl_props_body(edge_record, EDGE_REQUIRED_KEYS)

        return template.render(
            EDGE_TYPE=edge_record[EDGE_TYPE], PROPERTIES=properties)

    def make_alter_edgetype_command(
            self, edge_record: dict, cur_props: dict) -> str:
        """
        To make a ALTER EDGE command
        :param edge_record:
        :return:
        """
        target = {}
        for k, v in edge_record.items():
            if k in EDGE_REQUIRED_KEYS:
                continue

            prop_type = k.split(":")[-1]
            prop_name = k.removesuffix(f":{ prop_type }")
            target[prop_name] = prop_type
        involved = list(set(target.items()) - set(cur_props.items()))
        changed = [el for el in involved if el[0] in cur_props]
        added = [el for el in involved if el[0] not in cur_props]
        outdated = list(set(cur_props.items()) - set(target.items()))
        deleted = [el for el in outdated if el[0] not in target]

        template = Template(
            """
            {%- if changed %}ALTER EDGE {{ EDGE_TYPE }} CHANGE(
                {%- for el in changed %}{{ el[0] }} {{ el[1] }} NULL \
                    {{- ", " if not loop.last else "" }}
                {%- endfor %});
            {%- endif %}
            {%- if added %}ALTER EDGE {{ EDGE_TYPE }} ADD(
                {%- for el in added %}{{ el[0] }} {{ el[1] }} NULL \
                    {{- ", " if not loop.last else "" }}
                {%- endfor %});
            {%- endif %}
            {%- if deleted %}ALTER TAG {{ EDGE_TYPE }} DROP(
                {%- for el in deleted %}{{ el[0] }} {{ el[1] }} NULL \
                    {{- ", " if not loop.last else "" }}
                {%- endfor %});
            {%- endif %}\
        """
        )

        return template.render(
            EDGE_TYPE=edge_record[EDGE_TYPE],
            changed=changed,
            added=added,
            deleted=deleted,
        )

    def _alter_tag_schema(
            self, session: Session, vertex_record: dict) -> ResultSet:
        try:
            r_json_string = session.execute_json(
                f"DESCRIBE TAG { vertex_record[TAG_KEY] };"
            ).decode("utf-8")
        except Exception as e:
            LOGGER.debug(
                "Alter Tag Schema failed when getting existing TAG %s",
                vertex_record[TAG_KEY],
            )
            raise e

        cur_propertices = {
            p["row"][0]: p["row"][1]
            for p in json.loads(r_json_string)["results"][0]["data"]
        }
        cur_propertices.pop(PUBLISHED_TAG_PROPERTY_NAME)
        cur_propertices.pop(LAST_UPDATED_EPOCH_MS)

        query = self.make_alter_tag_command(vertex_record, cur_propertices)
        try:
            r = session.execute(query)
        except Exception as e:
            LOGGER.debug(
                "Alter Tag Schema failed when executing ALTER TAG %s",
                vertex_record[TAG_KEY],
            )
            raise e

        if r.is_suceeded():
            return r
        else:
            LOGGER.debug(
                "Alter Tag Schema: %s with error %s", query, r.error_msg())
            raise RuntimeError(f"Failed when altering tag schema: { query }")

    @retry_with_backoff()
    def _create_tag_schema(
            self, session: Session, vertex_record: dict) -> ResultSet:
        query = self.make_create_tag_command(vertex_record)
        try:
            r = session.execute(query)
        except Exception as e:
            LOGGER.debug(
                "CREATE Tag Schema failed when executing CREATE TAG %s",
                vertex_record[TAG_KEY],
            )
            raise e
        if r.is_suceeded():
            return r
        elif (r.error_code() == ErrorCode.E_EXECUTION_ERROR and
                NEBULA_EXISTED in r.error_msg().lower()):
            return self._alter_tag_schema(
                session=session, vertex_record=vertex_record)
        else:
            LOGGER.debug(
                "Create tag schema: %s with error %s", query, r.error_msg())
            raise RuntimeError(f"Failed when creating tag schema: { query }")

    def _alter_edgetype_schema(
            self, session: Session, edge_record: dict) -> ResultSet:
        try:
            r_json_string = session.execute_json(
                f"DESCRIBE EDGE { edge_record[EDGE_TYPE] };"
            ).decode("utf-8")
        except Exception as e:
            LOGGER.debug(
                "Alter Edge Schema failed when getting existing EDGE %s",
                edge_record[EDGE_TYPE],
            )
            raise e

        cur_propertices = {
            p["row"][0]: p["row"][1]
            for p in json.loads(r_json_string)["results"][0]["data"]
        }
        cur_propertices.pop(PUBLISHED_EDGE_PROPERTY_NAME)
        cur_propertices.pop(LAST_UPDATED_EPOCH_MS)

        query = self.make_alter_edgetype_command(edge_record, cur_propertices)
        try:
            r = session.execute(query)
        except Exception as e:
            LOGGER.debug(
                "Alter Edge Schema failed when executing ALTER EDGE %s",
                edge_record[TAG_KEY],
            )
            raise e

        if r.is_suceeded():
            return r
        else:
            LOGGER.debug(
                "Alter Edge Schema: %s with error %s", query, r.error_msg())
            raise RuntimeError(f"Failed when altering edge schema: { query }")

    @retry_with_backoff()
    def _create_edgetype_schema(
            self, session: Session, edge_record: dict) -> None:
        query = self.make_create_edgetype_command(edge_record)
        r = session.execute(query)
        if r.is_suceeded():
            return r
        elif (r.error_code() == ErrorCode.E_EXECUTION_ERROR and
                NEBULA_EXISTED in r.error_msg().lower()):
            return self._alter_edgetype_schema(
                session=session, edge_record=edge_record)
        else:
            LOGGER.debug(
                "Create edge type schema: %s with error %s",
                query, r.error_msg())
            raise RuntimeError(
                f"Failed when creating edge type schema: { query }")

    def _ensure_schema(self, session: Session) -> None:
        """
        Go over the files and create schema, it's assumped the csv record types
        are consistent across lines.
        :return:
        """
        LOGGER.info("Creating Tag Schema")

        for vertex_file in self._vertex_files:
            with open(vertex_file, "r", encoding="utf8") as vertex_csv:
                for vertex_record in pandas.read_csv(
                    vertex_csv, na_filter=False
                ).to_dict(orient="records"):
                    tag = vertex_record[TAG_KEY]
                    if tag not in self.tags:
                        self._create_tag_schema(session, vertex_record)
                        self.tags.add(tag)

        for edge_file in self._edge_files:
            with open(edge_file, "r", encoding="utf8") as edge_csv:
                for edge_record in pandas.read_csv(
                    edge_csv, na_filter=False).to_dict(
                    orient="records"
                ):
                    edge_type = edge_record[EDGE_TYPE]
                    if edge_type not in self.edge_types:
                        self._create_edgetype_schema(edge_record)
                        self.edge_types.add(edge_type)

    def _import_vertices(self, vertex_file: str, session: Session) -> None:
        """
        Iterate over the csv records of a file.

        Example of nGQL DML query executed by this method:
        INSERT VERTEX Column (name, order_pos, type) \
            VALUES "presto://gold.test_schema1/test_table1/test_id1": \
            ("n3", 2, "bigint");
        :param vertex_file:
        :return:
        """

        with open(vertex_file, "r", encoding="utf8") as vertex_csv:
            csv_buffer = list()
            for vertex_record in pandas.read_csv(
                vertex_csv, na_filter=False).to_dict(
                orient="records"
            ):
                csv_buffer.append(vertex_record)
                if len(csv_buffer) > self._nebula_insert_batchsize:
                    query = self.make_insert_vertex_command(
                        vertex_records=csv_buffer)
                    self._execute_query(query, session)
                    del csv_buffer[:]
            if csv_buffer:
                query = self.make_insert_vertex_command(
                    vertex_records=csv_buffer)
                self._execute_query(query, session)
                del csv_buffer[:]
        return

    def make_insert_vertex_command(self, vertex_records: List) -> str:
        """
        :param vertex_record:
        :return:
        """
        tag = vertex_records[0][TAG_KEY]
        prop_keys = list(set(vertex_records[0].keys) - TAG_REQUIRED_KEYS)
        property_list = list(filter(self._get_prop_name, prop_keys)) + (
            PUBLISHED_TAG_PROPERTY_NAME,
            LAST_UPDATED_EPOCH_MS,
        )
        properties = ", ".join(property_list)

        command_prefix = f"INSERT VERTEX { tag } ({ properties }) VALUES"

        prop_str_suffix = f',"{ self._publish_tag }",timestamp()'

        formated_records = list()
        for rec in vertex_records:
            prop_list = [
                self._quote(p) + rec[p] + self._quote(p) for p in prop_keys]
            formated_records.append(
                rec[VID_KEY], ",".join(prop_list) + prop_str_suffix)

        template = Template("""
            {%- for r in RECORDS %} "{{ r[0] }}":({{ r[1] }})
            {{ ", " if not loop.last else "" }}
            {%- endfor %});
        """)

        return command_prefix + template.render(RECORDS=formated_records)

    def _import_edges(self, edge_file: str, session: Session) -> None:
        """
        Iterate over the csv records of a file.

        Example of nGQL DML query executed by this method:
        INSERT EDGE COLUMN (START_LABEL, END_LABEL, REVERSE_TYPE) VALUES \
        "hive://gold.test_schema/test_table1"->
            "hive://gold.test_schema/test_table1/col1":\
        ("Table", "Column", "COLUMN_OF");

        :param edge_file:
        :return:
        """

        with open(edge_file, "r", encoding="utf8") as edge_csv:
            csv_buffer = list()
            for edge_record in pandas.read_csv(
                edge_csv, na_filter=False).to_dict(
                orient="records"
            ):
                csv_buffer.append(edge_record)
                if len(csv_buffer) > self._nebula_insert_batchsize:
                    query = self.make_insert_edge_command(
                        edge_records=csv_buffer)
                    self._execute_query(query, session)
                    del csv_buffer[:]
            if csv_buffer:
                query = self.make_insert_edge_command(
                    edge_records=csv_buffer)
                self._execute_query(query, session)
                del csv_buffer[:]
        return

    def make_insert_edge_command(self, edge_records: List) -> str:
        """
        :param edge_record:
        :return:
        """
        edge_type = edge_records[0][EDGE_TYPE]
        prop_keys = list(set(edge_records[0].keys) - EDGE_REQUIRED_KEYS)
        property_list = list(filter(self._get_prop_name, prop_keys)) + (
            PUBLISHED_TAG_PROPERTY_NAME,
            LAST_UPDATED_EPOCH_MS,
        )
        properties = ", ".join(property_list)

        command_prefix = f"INSERT edge { edge_type } ({ properties }) VALUES"

        prop_str_suffix = f',"{ self._publish_tag }", timestamp()'

        formated_records = list()
        for rec in edge_records:
            prop_list = [
                self._quote(p) + rec[p] + self._quote(p) for p in prop_keys]
            formated_records.append(
                rec[EDGE_START_KEY],
                rec[EDGE_END_KEY],
                ",".join(prop_list) + prop_str_suffix,
            )

        template = Template("""
            {%- for r in RECORDS %}"{{ r[0] }}"->"{{ r[1] }}":({{ r[1] }})
            {{ ", " if not loop.last else "" }}
            {%- endfor %});
        """)

        return command_prefix + template.render(RECORDS=formated_records)

    def _ddl_props_body(self, record_dict: dict, excludes: Set) -> str:
        """
        To generate DDL properties body.

        :param record_dict: A dict represents CSV row
        :param excludes: Set of excluded columns shouldnt be in properties
                         (e.g: KEY, LABEL ...)
        :return: Properties body for DDL statement
        """
        props = []
        for k, v in record_dict.items():
            if k in excludes:
                continue

            prop_type = k.split(":")[-1]
            prop_name = k.removesuffix(f":{ prop_type }")

            props.append(f"{ prop_name } { prop_type } NULL")

        props.append(f"{PUBLISHED_TAG_PROPERTY_NAME} string NOT NULL")
        props.append(f"{LAST_UPDATED_EPOCH_MS} timestamp NOT NULL")

        return ", ".join(props)

    @retry_with_backoff()
    def _execute_query(self, query: str, session: Session) -> ResultSet:
        """
        :param query:
        :param session:
        :return:
        """
        try:
            r = session.execute(query)
        except Exception as e:
            LOGGER.debug("Failed executing query: %s", query)
            raise e
        if r.is_suceeded():
            return r
        else:
            raise RuntimeError(f"Failed executing query: { query }")

    def _quote(self, prop):
        if prop.split(":")[-1] in NEBULA_UNQUOTED_TYPES:
            return '"'
        else:
            return ""

    def _get_prop_name(self, prop):
        prop_type = prop.split(":")[-1]
        prop_name = prop.removesuffix(f":{ prop_type }")
        return prop_name

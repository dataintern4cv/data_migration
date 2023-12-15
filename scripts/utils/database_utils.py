# Helper functions for database connections
import logging
import os
import time

import pandas as pd
import cx_Oracle
from sqlalchemy.exc import StatementError, DBAPIError

from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker

from scripts.utils.s3_data_utils import S3DataUtils
from scripts.utils.file_utils import read_config
from scripts.init_config import config


def get_engine_path(db: str, local_port):
    engine_path_auth = ''
    # create engine string with specific service
    if db == 'ORACLE':
        db_params = read_config(db)
        driver = db_params['DRIVER']
        user = db_params['USER']
        _pass = db_params['PASS']
        service = db_params['SERVICE']
        engine_path_auth = f"oracle+{driver}://{user}:{db_params['PASS']}@127.0.0.1:{local_port}/?service_name={service}"

    elif db == 'POSTGRE':
        db_params = read_config(db)
        user = db_params['USER']
        _pass = db_params['PASS']
        service = db_params['SERVICE']
        engine_path_auth = f'postgresql://{user}:{_pass}@127.0.0.1:{local_port}/{service}'

    return engine_path_auth


def get_ssh_tunnel(service: str) -> SSHTunnelForwarder:
    # get ssh params
    ssh_params = read_config(section='SSH')

    # get service params
    service_params = read_config(section=service)

    # assign params
    ssh_host = ssh_params['SSH_HOST']
    ssh_user = ssh_params['SSH_USER']
    private_key = ssh_params['PRIVATE_KEY']
    ssh_port = int(ssh_params['SSH_PORT'])
    bind_host = service_params['HOST']
    bind_port = int(service_params['PORT'])

    # Create an SSHTunnelForwarder object
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=private_key,
        remote_bind_address=(bind_host, bind_port)
    )

    return tunnel


def create_session(db: str, local_port: str):
    # get engine path
    engine_path_auth = get_engine_path(db, local_port)

    # create engine and session
    engine = create_engine(engine_path_auth)
    Session = sessionmaker(bind=engine)
    _session = Session()

    return _session


def execute_sql_file_to_df(service: str, file: str, order_date: str):
    config.setup_logging(logs='info')
    log = logging.getLogger(os.path.basename(__file__))

    tunnel = None
    session = None
    result_df = None

    # Read the SQL script from the file
    with open(file, 'r') as sql_file:
        sql_script = sql_file.read()

    # Replace the parameter in the SQL script with the actual order date
    sql_query = sql_script.replace(':target_order_date', f"'{order_date}'")

    try:
        # create tunnel
        tunnel = get_ssh_tunnel(service=service)
        tunnel.start()
        local_port = str(tunnel.local_bind_port)

        # create session
        session = create_session(db=service, local_port=local_port)

        try:
            # Execute the SQL query and fetch the result into a DataFrame
            result_df = pd.read_sql_query(sql_query, session.bind)

        except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
            log.error(f'Error executing SQL query: {e}', exc_info=True)
            session.rollback()  # Rollback changes in case of an error

    except Exception as e:
        log.error(f'Error setting up database {service} connection: {e}', exc_info=True)

    finally:
        if (session is not None):
            session.close()
        if (tunnel is not None):
            tunnel.stop()

    return result_df


def get_oracle_table_data_to_csv(table_name: str):
    # Initialize tunnel and session to None
    tunnel = None
    session = None
    log = logging.getLogger(os.path.basename(__file__))

    try:
        # create tunnel
        tunnel = get_ssh_tunnel(service='ORACLE')
        tunnel.start()
        local_port = str(tunnel.local_bind_port)

        # create session
        session = create_session(db='ORACLE', local_port=local_port)

        try:

            # select query
            _query = f"SELECT * FROM {table_name}"

            # save to dataframe
            result_df = pd.read_sql_query(_query, session.bind)

            # save to local csv file
            csv_file_path = os.path.join(config.DIR_CSV, f'{table_name}.csv')
            result_df.to_csv(csv_file_path, index=False, sep='|')

            log.info(f'Table data: {table_name} saved to {csv_file_path}')


        except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
            log.error(f'Error executing SQL query: {e}', exc_info=True)
            session.rollback()  # Rollback changes in case of an error

        finally:
            if session is not None:
                session.close()

    except Exception as e:
        log.error(f'Error setting up database ORACLE connection: {e}', exc_info=True)

    finally:
        try:
            if tunnel is not None:
                tunnel.stop()
                time.sleep(1)  # Add a short delay before exiting
        except Exception as e:
            log.error(f'Error stopping SSH tunnel: {e}', exc_info=True)



def load_data_file_s3_to_postgres_db(table_name: str):
    """
    Ingest data from S3 bucket to PostgreSQL database table

    :param table_name:

    """

    # Initialize tunnel and session to None
    tunnel = None
    log = logging.getLogger(os.path.basename(__file__))

    try:
        # Create tunnel
        tunnel = get_ssh_tunnel(service='POSTGRE')
        tunnel.start()
        local_port = str(tunnel.local_bind_port)

        # Create session
        session = create_session(db='POSTGRE', local_port=local_port)

        try:

            # Truncate data from table in PostgreSQL
            _query = f"TRUNCATE TABLE {table_name}"
            session.execute(text(_query))
            session.commit()

            try:

                # Insert into table in PostgreSQL
                df = S3DataUtils.download_from_s3_to_dataframe(table_name=table_name)
                df.to_sql(name=table_name, con=session.connection(), if_exists='append', index=False)
                session.commit()

            except (exc.SQLAlchemyError, pd.errors.EmptyDataError) as e:
                log.error(f'Error writing DataFrame to PostgreSQL: {e}', exc_info=True)
                session.rollback()

        except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
            log.error(f'Error executing SQL query: {e}', exc_info=True)
            session.rollback()  # Rollback changes in case of an error

        finally:
            if session is not None:
                session.close()

    except Exception as e:
        log.error(f'Error setting up database connection: {e}', exc_info=True)

    finally:
        try:
            if tunnel is not None:
                tunnel.stop()
                time.sleep(1)  # Add a short delay before exiting
        except Exception as e:
            log.error(f'Error stopping SSH tunnel: {e}', exc_info=True)


def get_dataframe_from_database_table(database: str, table_name: str ):

    result_df = None
    tunnel = None
    log = logging.getLogger(os.path.basename(__file__))

    try:
        # create tunnel
        tunnel = get_ssh_tunnel(service=database)
        tunnel.start()
        local_port = str(tunnel.local_bind_port)

        # create session
        session = create_session(db=database, local_port=local_port)

        try:

            # generate
            _query = f"SELECT * FROM {table_name}"

            # save to dataframe
            result_df = pd.read_sql_query(_query, session.bind)

            log.info(f'Table data: {table_name} saved to DataFrame')

        except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
            log.error(f'Error executing SQL query: {e}', exc_info=True)
            session.rollback()  # Rollback changes in case of an error

        finally:
            if session is not None:
                session.close()

    except Exception as e:
        log.error(f'Error setting up database ORACLE connection: {e}', exc_info=True)

    finally:
        try:
            if tunnel is not None:
                tunnel.stop()
                time.sleep(1)  # Add a short delay before exiting
        except Exception as e:
            log.error(f'Error stopping SSH tunnel: {e}', exc_info=True)

    return result_df


 # Helper functions for database connections
import logging
import os

from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker

from scripts.utils.file_utils import read_config
from scripts.init_config import config

config.setup_logging('error')
log = logging.getLogger(os.path.basename(__file__))

def get_ssh_tunnel(service: str) -> SSHTunnelForwarder:

    #get ssh params
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

    # create tunnel

    # Create an SSHTunnelForwarder object
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=private_key,
        remote_bind_address=(bind_host, bind_port)
    )
    return tunnel

def create_session(db: str, local_port: str):

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

    # create engine and session
    engine = create_engine(engine_path_auth)
    _session_obj = sessionmaker(bind=engine)
    _session = _session_obj()

    return _session


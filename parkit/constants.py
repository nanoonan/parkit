import multiprocessing

from parkit.typeddicts import Profiles

PID_FILENAME_PREFIX: str = 'parkit-'
PID_FILENAME_EXTENSION: str = '.pid'

CLUSTER_STORAGE_PATH_ENVNAME: str = 'PARKIT_CLUSTER_STORAGE_PATH'
NODE_UID_ENVNAME: str = 'PARKIT_NODE_UID'
CLUSTER_UID_ENVNAME: str = 'PARKIT_CLUSTER_UID'

DEFAULT_PROCESS_TERMINATION_TIMEOUT: float = 1.
DEFAULT_POOL_SIZE: int = min(max(4, multiprocessing.cpu_count()), 8)
DEFAULT_MONITOR_POLLING_INTERVAL: float = 5.
DEFAULT_WORKER_POLLING_INTERVAL: float = 0.1
DEFAULT_ADAPTER_POLLING_INTERVAL: float = 0.05

PROCESS_TERMINATION_TIMEOUT_ENVNAME: str = 'PARKIT_PROCESS_TERMINATION_TIMEOUT'
POOL_SIZE_ENVNAME: str = 'PARKIT_POOL_SIZE'
MONITOR_POLLING_INTERVAL_ENVNAME: str = 'PARKIT_MONITOR_POLLING_INTERVAL'
WORKER_POLLING_INTERVAL_ENVNAME: str = 'PARKIT_WORKER_POLLING_INTERVAL'
ADAPTER_POLLING_INTERVAL_ENVNAME: str = 'PARKIT_ADAPTER_POLLING_INTERVAL'

SELF_ENVNAME: str = 'PARKIT_SELF_REFERENCE'

ANONYMOUS_SCOPE_FLAG_ENVNAME: str = 'PARKIT_GC_SCOPE'

PARKIT_TEMP_SITE_DIRNAME: str = 'parkit'
GLOBAL_SITE_STORAGE_PATH_ENVNAME: str = 'PARKIT_GLOBAL_SITE_STORAGE_PATH'
GLOBAL_FILE_LOCK_PATH_ENVNAME: str =  'PARKIT_GLOBAL_FILE_LOCK_PATH'
GLOBAL_FILE_LOCK_FILENAME: str = 'parkit.global.lock'

PROCESS_UUID_ENVNAME: str = 'PARKIT_PROCESS_UUID'

LMDB_WRITE_MAP_ENVNAME: str = 'LMDB_WRITE_MAP'
LMDB_MAP_ASYNC_ENVNAME: str = 'LMDB_MAP_ASYNC'
LMDB_METASYNC_ENVNAME: str = 'LMDB_METASYNC'
LMDB_MAX_DBS_ENVNAME: str = 'LMDB_MAX_DBS'
LMDB_READONLY_ENVNAME: str = 'LMDB_READONLY'
LMDB_SYNC_ENVNAME: str = 'LMDB_SYNC'
LMDB_READAHEAD_ENVNAME: str = 'LMDB_READAHEAD'
LMDB_MEMINIT_ENVNAME: str = 'LMDB_MEMINIT'
LMDB_MAX_SPARE_TXNS_ENVNAME: str = 'LMDB_MAX_SPARE_TXNS'
LMDB_MAX_READERS_ENVNAME: str = 'LMDB_MAX_READERS'
LMDB_PROFILE_ENVNAME: str = 'LMDB_PROFILE'
LMDB_INITIAL_MAP_SIZE_ENVNAME: str = 'LMDB_INITIAL_MAP_SIZE'

LMDB_PROFILES: Profiles = dict(
	default = {
		'LMDB_INITIAL_MAP_SIZE': 1073741824,
		'LMDB_WRITE_MAP': True,
		'LMDB_METASYNC': True,
		'LMDB_MAP_ASYNC': True,
		'LMDB_MAX_DBS': 65536,
		'LMDB_READONLY': False,
		'LMDB_SYNC': True,
		'LMDB_READAHEAD': False,
		'LMDB_MEMINIT': False,
		'LMDB_MAX_SPARE_TXNS': multiprocessing.cpu_count(),
		'LMDB_MAX_READERS': 64
	}
)

ENVIRONMENT_UUID_KEY: str = '__uuid__'

ROOT_NAMESPACE: str = ''
DEFAULT_NAMESPACE: str = 'default'
TASK_NAMESPACE: str = '__task__'

SCOPE_TABLE_NAME: str = '__scope_table__'

TASK_QUEUE_PATH: str = '__task__/__task_queue__'
NODE_TERMINATION_QUEUE_PATH: str = '__task__/__node_termination_queue__'
POOL_STATE_DICT_PATH: str = '__task__/__pool_state_dict__'
SYSLOG_PATH: str = '__syslog__'
PID_TABLE_DICT_PATH: str = '__pid_table__'

ATTRIBUTE_DATABASE: str = '__attribute__'
VERSION_DATABASE: str = '__version__'
NAME_DATABASE: str = '__name__'
DESCRIPTOR_DATABASE: str = '__descriptor__'

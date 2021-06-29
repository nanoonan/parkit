import multiprocessing

from parkit.typeddicts import Profiles

RUNNING_CACHE_MAXSIZE = 4096
RUNNING_CACHE_TTL = 1.

PROCESS_UID_ENVNAME: str = 'PARKIT_PROCESS_UID'

KEY_SUFFIX_OBJECT_BINARY_ATTRIBUTE: str = '_binary'

NODE_UID_ENVNAME: str = 'PARKIT_NODE_UID'
CLUSTER_UID_ENVNAME: str = 'PARKIT_CLUSTER_UID'

DEFAULT_PROCESS_TERMINATION_TIMEOUT: float = 1.
DEFAULT_CLUSTER_CONCURRENCY: int = min(max(4, multiprocessing.cpu_count()), 8)
DEFAULT_MONITOR_POLLING_INTERVAL: float = 5.
DEFAULT_WORKER_POLLING_INTERVAL: float = 0.02
DEFAULT_ADAPTER_POLLING_INTERVAL: float = 0.05
DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL: float = 1.
DEFAULT_MAX_SYSLOG_ENTRIES: int = 100000

MAX_SYSLOG_ENTRIES_ENVNAME: str = 'PARKIT_MAX_SYSLOG_ENTRIES'
PROCESS_TERMINATION_TIMEOUT_ENVNAME: str = 'PARKIT_PROCESS_TERMINATION_TIMEOUT'
CLUSTER_CONCURRENCY_ENVNAME: str = 'PARKIT_CLUSTER_CONCURRENCY'
MONITOR_POLLING_INTERVAL_ENVNAME: str = 'PARKIT_MONITOR_POLLING_INTERVAL'
WORKER_POLLING_INTERVAL_ENVNAME: str = 'PARKIT_WORKER_POLLING_INTERVAL'
ADAPTER_POLLING_INTERVAL_ENVNAME: str = 'PARKIT_ADAPTER_POLLING_INTERVAL'
SCHEDULER_HEARTBEAT_INTERVAL_ENVNAME: str = 'PARKIT_SCHEDULER_HEARTBEAT_INTERVAL'

SELF_ENVNAME: str = 'PARKIT_SELF_REFERENCE'

PARKIT_TEMP_SITE_DIRNAME: str = 'parkit'
GLOBAL_SITE_STORAGE_PATH_ENVNAME: str = 'PARKIT_GLOBAL_SITE_STORAGE_PATH'
GLOBAL_FILE_LOCK_PATH_ENVNAME: str =  'PARKIT_GLOBAL_FILE_LOCK_PATH'
GLOBAL_FILE_LOCK_FILENAME: str = 'parkit.global.lock'

DEFAULT_SITE_PATH_ENVNAME: str = 'PARKIT_DEFAULT_SITE_PATH'

MONITOR_DAEMON_MODULE: str = 'parkit.daemons.monitor'
WORKER_DAEMON_MODULE: str = 'parkit.daemons.worker'
SCHEDULER_DAEMON_MODULE: str = 'parkit.daemons.scheduler'

LMDB_PROFILES: Profiles = dict(
	default = {
		'LMDB_INITIAL_MAP_SIZE': 134217728,
		'LMDB_WRITE_MAP': True,
		'LMDB_METASYNC': True,
		'LMDB_MAP_ASYNC': True,
		'LMDB_MAX_DBS': 65536,
		'LMDB_READONLY': False,
		'LMDB_SYNC': True,
		'LMDB_READAHEAD': False,
		'LMDB_MEMINIT': False,
		'LMDB_MAX_SPARE_TXNS': 256,
		'LMDB_MAX_READERS': 256
	},
	memory = {
		'LMDB_INITIAL_MAP_SIZE': 134217728,
		'LMDB_WRITE_MAP': False,
		'LMDB_METASYNC': False,
		'LMDB_MAP_ASYNC': False,
		'LMDB_MAX_DBS': 65536,
		'LMDB_READONLY': False,
		'LMDB_SYNC': False,
		'LMDB_READAHEAD': True,
		'LMDB_MEMINIT': False,
		'LMDB_MAX_SPARE_TXNS': 256,
		'LMDB_MAX_READERS': 256
	}
)

ENVIRONMENT_UUID_KEY: str = '__uuid__'

ROOT_NAMESPACE: str = ''
DEFAULT_NAMESPACE: str = 'default'
MEMORY_NAMESPACE: str = 'memory'
SCHEDULER_NAMESPACE: str = '__sched__'
TASK_NAMESPACE: str = '__task__'
MODULE_NAMESPACE: str = 'module'

SUBMIT_QUEUE_PATH: str = '__task__/__submit_queue__'
NODE_TERMINATION_QUEUE_PATH: str = '__task__/__node_termination_queue__'
CLUSTER_STATE_DICT_PATH: str = '__task__/__cluster_state_dict__'
SYSLOG_PATH: str = 'memory/syslog/__syslog__'
PIDTABLE_DICT_PATH: str = 'memory/pidtable/__pidtable__'

ATTRIBUTE_DATABASE: str = '__attribute__'
VERSION_DATABASE: str = '__version__'
NAME_DATABASE: str = '__name__'
DESCRIPTOR_DATABASE: str = '__descriptor__'

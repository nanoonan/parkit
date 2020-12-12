
MMH3_SEED = 42

INSTALL_PATH_ENVNAME = 'PARKIT_INSTALL_PATH'
INSTALLATION_UUID_ENVNAME = 'PARKIT_INSTALLATION_UUID'
PROCESS_INSTANCE_UUID_ENVNAME = 'PARKIT_PROCESS_UUID'
LMDB_WRITE_MAP_ENVNAME = 'LMDB_WRITE_MAP'
LMDB_MAP_ASYNC_ENVNAME = 'LMDB_MAP_ASYNC'
LMDB_MAP_SIZE_ENVNAME = 'LMDB_MAP_SIZE'
LMDB_MAX_DBS_ENVNAME = 'LMDB_MAX_DBS'
LMDB_READONLY_ENVNAME = 'LMDB_READONLY'
LMDB_SYNC_ENVNAME = 'LMDB_SYNC'
LMDB_READAHEAD_ENVNAME = 'LMDB_READAHEAD'
LMDB_MEMINIT_ENVNAME = 'LMDB_MEMINIT'
LMDB_MAX_SPARE_TXNS_ENVNAME = 'LMDB_MAX_SPARE_TXNS'
LMDB_MAX_READERS_ENVNAME = 'LMDB_MAX_READERS'
LMDB_PROFILE_ENVNAME = 'LMDB_PROFILE'

lmdb_profiles = dict(
	default = [
		(LMDB_WRITE_MAP_ENVNAME, False),
		(LMDB_MAP_ASYNC_ENVNAME, False),
		(LMDB_MAP_SIZE_ENVNAME, 10485760000),
		(LMDB_MAX_DBS_ENVNAME, 4096),
		(LMDB_READONLY_ENVNAME, False),
		(LMDB_SYNC_ENVNAME, True),
		(LMDB_READAHEAD_ENVNAME, False),
		(LMDB_MEMINIT_ENVNAME, False),
		(LMDB_MAX_SPARE_TXNS_ENVNAME, 256),
		(LMDB_MAX_READERS_ENVNAME, 256)
	],
	safety = [
		(LMDB_WRITE_MAP_ENVNAME, False),
		(LMDB_MAP_ASYNC_ENVNAME, False),
		(LMDB_MAP_SIZE_ENVNAME, 10485760000),
		(LMDB_MAX_DBS_ENVNAME, 4096),
		(LMDB_READONLY_ENVNAME, False),
		(LMDB_SYNC_ENVNAME, True),
		(LMDB_READAHEAD_ENVNAME, False),
		(LMDB_MEMINIT_ENVNAME, True),
		(LMDB_MAX_SPARE_TXNS_ENVNAME, 256),
		(LMDB_MAX_READERS_ENVNAME, 256)
	],
	performance = [
		(LMDB_WRITE_MAP_ENVNAME, False),
		(LMDB_MAP_ASYNC_ENVNAME, True),
		(LMDB_MAP_SIZE_ENVNAME, 10485760000),
		(LMDB_MAX_DBS_ENVNAME, 4096),
		(LMDB_READONLY_ENVNAME, False),
		(LMDB_SYNC_ENVNAME, True),
		(LMDB_READAHEAD_ENVNAME, False),
		(LMDB_MEMINIT_ENVNAME, False),
		(LMDB_MAX_SPARE_TXNS_ENVNAME, 256),
		(LMDB_MAX_READERS_ENVNAME, 256)
	]
)

DEFAULT_NAMESPACE = 'default'

METADATA_DATABASE_NAME = '__metadata__'
VERSION_DATABASE_NAME = '__version__'

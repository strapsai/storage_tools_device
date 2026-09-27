__version_info__ = (1, 1, 0)
__version__ = ".".join(map(str, __version_info__))

# Wire-protocol version shared with storage_tools_server. Both sides refuse to talk to a peer
# with a different number and say so on their web pages (see Device.test_connection).
#   1: upload ids derived from the file path on the device (implicit, before this constant existed)
#   2: upload ids derived from the file's content hash; the device announces protocol/version on join
PROTOCOL_VERSION = 2

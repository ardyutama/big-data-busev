# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: canbus_test.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x11\x63\x61nbus_test.proto\x12\x05proto\x1a\x1bgoogle/protobuf/empty.proto\"M\n\rCANBusMessage\x12\x0e\n\x06\x63\x61n_id\x18\x01 \x01(\x0c\x12\x0b\n\x03\x64lc\x18\x02 \x01(\x05\x12\x0c\n\x04\x64\x61ta\x18\x03 \x01(\x0c\x12\x11\n\ttimestamp\x18\x04 \x01(\x03\"7\n\x11\x43\x41NBusMessageList\x12\"\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x14.proto.CANBusMessage2G\n\rCANBusService\x12\x36\n\x04Send\x12\x14.proto.CANBusMessage\x1a\x16.google.protobuf.Empty\"\x00\x42\x13Z\x11\x62us-project/protob\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'canbus_test_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z\021bus-project/proto'
  _CANBUSMESSAGE._serialized_start=57
  _CANBUSMESSAGE._serialized_end=134
  _CANBUSMESSAGELIST._serialized_start=136
  _CANBUSMESSAGELIST._serialized_end=191
  _CANBUSSERVICE._serialized_start=193
  _CANBUSSERVICE._serialized_end=264
# @@protoc_insertion_point(module_scope)

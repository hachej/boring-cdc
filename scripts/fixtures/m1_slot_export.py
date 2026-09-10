#!/usr/bin/env python3
"""Minimal trust-auth replication-protocol CREATE_REPLICATION_SLOT fixture."""
import socket,struct,sys
if len(sys.argv)!=3: raise SystemExit('usage: m1_slot_export.py HOST PORT')
s=socket.create_connection((sys.argv[1],int(sys.argv[2])),timeout=10)
def cstr(x): return x.encode()+b'\0'
payload=struct.pack('!I',196608)+cstr('user')+cstr('boring_cdc_capture_bootstrap')+cstr('database')+cstr('postgres')+cstr('replication')+cstr('database')+b'\0'
s.sendall(struct.pack('!I',len(payload)+4)+payload)
def recv(n):
 out=b''
 while len(out)<n:
  chunk=s.recv(n-len(out))
  if not chunk: raise SystemExit('E_PROTOCOL_EOF')
  out+=chunk
 return out
while True:
 typ=recv(1); length=struct.unpack('!I',recv(4))[0]; body=recv(length-4)
 if typ==b'E': raise SystemExit('E_STARTUP:'+body.decode(errors='replace'))
 if typ==b'Z': break
query="CREATE_REPLICATION_SLOT boring_cdc_slot LOGICAL pgoutput (SNAPSHOT 'export')"
s.sendall(b'Q'+struct.pack('!I',len(query)+5)+query.encode()+b'\0')
row=None
while True:
 typ=recv(1); length=struct.unpack('!I',recv(4))[0]; body=recv(length-4)
 if typ==b'E': raise SystemExit('E_CREATE_SLOT:'+body.decode(errors='replace'))
 if typ==b'D':
  count=struct.unpack('!H',body[:2])[0]; pos=2; vals=[]
  for _ in range(count):
   n=struct.unpack('!i',body[pos:pos+4])[0]; pos+=4; vals.append(None if n<0 else body[pos:pos+n].decode()); pos += max(n,0)
  row=vals
 if typ==b'Z': break
s.close()
if not row or row[0]!='boring_cdc_slot' or not row[1] or not row[2] or row[3]!='pgoutput': raise SystemExit('E_EXPORT_SNAPSHOT_RESPONSE')
print('PASS slot=boring_cdc_slot consistent_point=present snapshot=present plugin=pgoutput')

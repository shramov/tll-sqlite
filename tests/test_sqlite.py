#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import os
import sqlite3

import pytest

from tll.channel import Context
from tll.error import TLLError
from tll.test_util import Accum

#import tll.logger

#tll.logger.init()

SCHEME = '''yamls://
- name: ignored
  fields:
    - {name: s0, type: '**int8'}
- name: scalar
  id: 10
  fields:
    - {name: i8, type: int8, options.sql.index: unique}
    - {name: i16, type: int16, options.sql.index: yes}
    - {name: i32, type: int32}
    - {name: i64, type: int64}
    - {name: u8, type: uint8}
    - {name: u16, type: uint16}
    - {name: u32, type: uint32}
    - {name: d, type: double}
- name: text
  id: 20
  fields:
    - {name: b, type: byte8}
    - {name: f, type: byte32, options.type: string}
    - {name: s, type: string, options.sql.primary-key: true}
- name: ptr_text
  id: 30
  fields:
    - {name: b, type: string}
    - {name: f, type: byte8}
    - {name: s, type: string, options.sql.primary-key: true}
    - {name: r, type: byte32, options.type: string}
    - {name: t, type: string}
'''

@pytest.fixture
def context():
    ctx = Context()
    ctx.load(os.path.join(os.environ.get("BUILD_DIR", "build"), "tll-sqlite"), 'channel_module')
    return ctx

@pytest.fixture
def db_file(tmp_path):
    return str(tmp_path / 'test.db')

def test_insert_unique(context, db_file):
    db = sqlite3.connect(db_file)
    c = context.Channel(f'sqlite://{db_file};replace=false;seq-index=unique', scheme=SCHEME, dump='scheme')
    c.open()

    c.post(name='scalar', data={'i8':-8, 'i16':-16, 'i32':-32, 'i64':-64, 'u8':8, 'u16':16, 'u32':32, 'd':1.23}, seq=1)
    with pytest.raises(TLLError): c.post(name='scalar', data={}, seq=1)
    with pytest.raises(TLLError): c.post(name='scalar', data={'i8':-8}, seq=2)

    assert list(db.cursor().execute('SELECT * FROM `scalar`')) == [(1, -8, -16, -32, -64, 8, 16, 32, 1.23)]

    c.post(name='text', data={'b':b'bytes', 'f':'fixed string', 's':'offset string'}, seq=1)

    assert list(db.cursor().execute('SELECT * FROM `text`')) == [(1, b'bytes\0\0\0', 'fixed string', 'offset string')]

def test_replace(context, db_file):
    db = sqlite3.connect(db_file)
    c = context.Channel(f'sqlite://{db_file};replace=true;seq-index=unique', scheme=SCHEME, dump='scheme')
    c.open()

    c.post(name='scalar', data={'i8':-8, 'i16':-16, 'i32':-32, 'i64':-64, 'u8':8, 'u16':16, 'u32':32, 'd':1.23}, seq=1)
    assert list(db.cursor().execute('SELECT * FROM `scalar`')) == [(1, -8, -16, -32, -64, 8, 16, 32, 1.23)]

    c.post(name='scalar', data={'i8': 100}, seq=1)

    assert list(db.cursor().execute('SELECT * FROM `scalar`')) == [(1, 100, 0, 0, 0, 0, 0, 0, 0)]

    c.post(name='text', data={'b':b'bytes', 'f':'fixed string', 's':'key'}, seq=1)

    assert list(db.cursor().execute('SELECT * FROM `text`')) == [(1, b'bytes\0\0\0', 'fixed string', 'key')]

    c.post(name='text', data={'b':b'other', 'f':'other string', 's':'key'}, seq=2)

    assert list(db.cursor().execute('SELECT * FROM `text`')) == [(2, b'other\0\0\0', 'other string', 'key')]

def test_insert(context, db_file):
    db = sqlite3.connect(db_file)
    c = context.Channel(f'sqlite://{db_file};replace=false;seq-index=no', scheme=SCHEME, dump='scheme')
    c.open()

    c.post(name='scalar', data={'i8':-8, 'i16':-16, 'i32':-32, 'i64':-64, 'u8':8, 'u16':16, 'u32':32, 'd':1.23})
    assert list(db.cursor().execute('SELECT * FROM `scalar`')) == [(0, -8, -16, -32, -64, 8, 16, 32, 1.23)]

    c.post(name='scalar', data={'i8':-9, 'i16':-17, 'i32':-33, 'i64':-65, 'u8':9, 'u16':17, 'u32':33, 'd':2.34})

    assert list(db.cursor().execute('SELECT * FROM `scalar`')) == [
        (0, -8, -16, -32, -64, 8, 16, 32, 1.23),
        (0, -9, -17, -33, -65, 9, 17, 33, 2.34),
    ]

    c.post(name='text', data={'b':b'bytes', 'f':'fixed string', 's':'key'})

    assert list(db.cursor().execute('SELECT * FROM `text`')) == [(0, b'bytes\0\0\0', 'fixed string', 'key')]

    with pytest.raises(TLLError): c.post(name='text', data={'b':b'other', 'f':'other string', 's':'key'})
    c.post(name='text', data={'b':b'other', 'f':'other string', 's':'key-1'})

    assert list(db.cursor().execute('SELECT * FROM `text`')) == [
        (0, b'bytes\0\0\0', 'fixed string', 'key'),
        (0, b'other\0\0\0', 'other string', 'key-1'),
    ]

BULK = '''yamls://
- name: msg 
  id: 10
  fields:
    - {name: field, type: int32}
'''

def test_bulk(context, db_file):
    db = sqlite3.connect(db_file)
    c = context.Channel(f'sqlite://{db_file};replace=false;bulk-size=10', scheme=BULK, dump='scheme')
    c.open()

    for i in range(5):
        c.post(name='msg', data={'field': i}, seq=i)

    assert list(db.cursor().execute('SELECT * FROM `msg`')) == []

    for i in range(5, 10):
        c.post(name='msg', data={'field': i}, seq=i)

    assert list(db.cursor().execute('SELECT * FROM `msg`')) == [(i, i) for i in range(10)]

    for i in range(10, 15):
        c.post(name='msg', data={'field': i}, seq=i)

    assert list(db.cursor().execute('SELECT * FROM `msg`')) == [(i, i) for i in range(10)]

    c.close()

    assert list(db.cursor().execute('SELECT * FROM `msg`')) == [(i, i) for i in range(15)]

REMAP = '''yamls://
- name: msg
  options.sql.table: table
  id: 10
  fields:
    - {name: field, type: int32}
'''

def test_remap(context, db_file):
    db = sqlite3.connect(db_file)
    c = context.Channel(f'sqlite://{db_file};replace=false', scheme=REMAP, dump='scheme')
    c.open()

    c.post(name='msg', data={}, seq=100)

    assert list(db.cursor().execute('SELECT * FROM `table`')) == [(100, 0)]

def test_query(context, db_file):
    db = sqlite3.connect(db_file)
    c = Accum(f'sqlite://{db_file};replace=false', scheme=BULK, dump='scheme', context=context)
    c.open()

    for i in range(5):
        c.post(name='msg', data={'field': i + 1}, seq=i)

    c.close()
    c.open(table='msg')

    for i in range(5):
        c.process()
        assert [(x.msgid, x.seq, c.unpack(x).as_dict()) for x in c.result] ==\
            [(10, j, {"field": j + 1}) for j in range(i + 1)]

def test_query_text(context, db_file):
    db = sqlite3.connect(db_file)
    c = Accum(f'sqlite://{db_file};replace=false', scheme=SCHEME, dump='scheme', context=context)
    c.open()

    for i in range(5):
        c.post(name='ptr_text', data={'b': f'b{i}', 'f': f'f{i}'.encode('ascii'), 's': f's{i}',
                                  'r': f'rrr{i}', 't': 't' * i}, seq=i)

    c.close()
    c.open(table='ptr_text')

    for i in range(5):
        c.process()
        assert [(x.msgid, x.seq) for x in c.result] == [(30, j) for j in range(i + 1)]
        data = c.unpack(c.result[i]).as_dict()
        assert data['b'] == f'b{i}'
        assert data['f'] == bytearray((f'f{i}' + '\0' * (7 - len(str(i)))).encode('ascii'))
        assert data['s'] == f's{i}'
        assert data['r'] == f'rrr{i}'
        assert data['t'] == 't' * i

    c.process()

    assert c.result[-1].msgid == c.scheme_control['data_end'].msgid
    assert c.result[-1].type == c.result[-1].Type.Control

    c.close()

def test_control_message(context, db_file):
    db = sqlite3.connect(db_file)
    c = Accum(f'sqlite://{db_file};replace=false', scheme=SCHEME, dump='scheme', context=context)
    c.open()

    for i in range(5):
        c.post(name='scalar', data={'i8':i, 'i16':-17, 'i32':-33, 'i64':-65, 'u8':9,
                                    'u16':17, 'u32':33, 'd':2.34}, seq=i)
        c.post(name='text', data={'b':b'bytes', 'f':'fixed string', 's':f'offset string {i}'}, seq=i)

    c.close()
    c.open(table='scalar')

    for i in range(5):
        c.process()
    c.process()
    assert c.result[-1].msgid == c.scheme_control['data_end'].msgid
    assert c.result[-1].type == c.result[-1].Type.Control

    c.close()
    c.open(table='text')
    for i in range(5):
        c.process()
    c.process()
    assert c.result[-1].msgid == c.scheme_control['data_end'].msgid
    assert c.result[-1].type == c.result[-1].Type.Control

    assert len(c.result) == 12
    c.close()
    c.open()
    c.post(type=c.Type.Control, data=c.scheme_control['table_name'].object(msgid=10))
    for i in range(5):
        c.process()
    c.process()
    assert c.result[-1].msgid == c.scheme_control['data_end'].msgid
    assert c.result[-1].type == c.result[-1].Type.Control
    assert len(c.result) == 18

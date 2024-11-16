#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import pytest
import os

from tll.channel import Context
from tll.test_util import *

SCHEME = '''yamls://
- name: header
  options.key: s0
  fields:
    - {name: s0, type: int8}
    - {name: s1, type: string}
- name: msg
  options.key: header.s0
  id: 10
  fields:
    - {name: header, type: header}
    - {name: f0, type: int8}
    - {name: f1, type: double}
'''

@pytest.fixture
def context():
    ctx = Context()
    ctx.load(os.path.join(os.environ.get("BUILD_DIR", "build"), "tll-jsqlite"))
    return ctx

@pytest.fixture
def db_file(tmp_path):
    return str(tmp_path / 'test.db')

data = [{'header':{'s0':10, 's1':'first'}, 'f0': 1, 'f1': 10.1}
       ,{'header':{'s0':10, 's1':'second'}, 'f0': 1, 'f1': 11.1}
       ,{'header':{'s0':20, 's1':'first'}, 'f0': 1, 'f1': 20.1}
       ]

@pytest.mark.parametrize("query,check", [
    ({}, [1,2]),
    ({'query':'msg'}, [1,2]),
    ({'query.header.s1':'first'}, [2]),
    ({'query.header.s1':'second'}, [1]),
    ({'query': 'msg', 'query.f0':'1'}, [1,2]),
    ({'query': 'msg', 'query.header.s0':'10'}, [1]),
])
def test(context, db_file, query, check):
    c = Accum(f'jsqlite://{db_file};dir=rw', scheme=SCHEME, table='test', context=context, name='master')
    #c = Accum('jsqlite://:memory:;dir=rw', scheme=SCHEME, table='test')
    c.open()
    for s,d in enumerate(data):
        c.post(d, name='msg', seq=s)

    c.close()

    ci = Accum(f'jsqlite://{db_file};dir=r', scheme=SCHEME, table='test', dump='scheme', autoclose='yes', name='client', context=context)
    ci.open(**query)
    scheme = ci.scheme
    for _ in range(10):
        ci.process()
    assert len(ci.result) == len(check)
    for m,c in zip([x for x in ci.result if x.type == x.Type.Data], check):
        r = scheme.unpack(m)
        assert r.as_dict() == data[c]

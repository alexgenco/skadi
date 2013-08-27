import collections as c
import copy

from skadi import index as i
from skadi.io import bitstream as b_io
from skadi.io.protobuf import demo as d_io
from skadi.io.protobuf import packet as p_io
from skadi.protoc import demo_pb2 as pb_d
from skadi.io.unpacker import entity as u_ent
from skadi.io.unpacker import string_table as u_st
from skadi.engine import world as w
from skadi.engine.observer import active_modifier as o_am
from skadi.protoc import netmessages_pb2 as pb_n


Snapshot = c.namedtuple('Snapshot', ['tick', 'modifiers', 'world'])


def construct(*args):
  return Demo(*args)


class Stream(object):
  def __init__(self, io, cb, rt, st, modifiers, world, stragglers=None):
    self.io = io
    self.class_bits = cb
    self.recv_tables = rt
    self.modifiers = st
    self.string_tables = st
    self.world = world
    self._baseline_cache = {}

    for peek, message in stragglers:
      pbmsg = d_io.parse(peek.kind, peek.compressed, message)
      self.advance(peek.tick, pbmsg)

  def __iter__(self):
    demo_io = d_io.construct(self.io)
    iter_entries = iter(demo_io)

    def advance():
      try:
        peek, message = next(iter_entries)

        if peek.kind == pb_d.DEM_FullPacket:
          return advance() # skip
        elif peek.kind == pb_d.DEM_Stop:
          raise StopIteration()

        pbmsg = d_io.parse(peek.kind, peek.compressed, message)
        return self.advance(peek.tick, pbmsg)
      except StopIteration:
        return None

    return iter(advance, None)

  def advance(self, tick, pbmsg):
    st, cb, rt = self.string_tables, self.class_bits, self.recv_tables

    index = i.construct(p_io.construct(pbmsg.data))
    upd_st = index.find_all(pb_n.svc_UpdateStringTable)

    for _pbmsg in [p_io.parse(p.kind, m) for p, m in upd_st]:
      key = self.string_tables.keys()[_pbmsg.table_id]
      _st = self.string_tables[key]

      ne = _pbmsg.num_changed_entries
      eb, sf, sb = _st.entry_bits, _st.size_fixed, _st.size_bits
      bs = b_io.construct(_pbmsg.string_data)

      for entry in u_st.unpack(bs, ne, eb, sf, sb):
        _st.update(entry)

    p, m = index.find(pb_n.svc_PacketEntities)
    pe = p_io.parse(p.kind, m)
    ct = pe.updated_entries
    bs = b_io.construct(pe.entity_data)

    unpacker = u_ent.unpack(bs, -1, ct, False, cb, self.world)

    for index, mode, context in unpacker:
      if mode & u_ent.PVS.Entering:
        cls, serial, diff = context

        if cls not in self._baseline_cache:
          data = st['instancebaseline'].get(cls)[1]
          bs = b_io.construct(data)
          unpacker = u_ent.unpack(bs, -1, 1, False, cb, self.world)

          self._baseline_cache[cls] = unpacker.unpack_baseline(rt[cls])

        state = dict(self._baseline_cache[cls])
        state.update(diff)

        self.world.create(cls, index, serial, state)
      elif mode & u_ent.PVS.Deleting:
        self.world.delete(index)
      elif mode ^ u_ent.PVS.Leaving:
        state = dict(self.world.find_index(index))
        state.update(context)

        self.world.update(index, state)

    return Snapshot(tick, self.modifiers, self.world)


class Demo(object):
  def __init__(self, prologue, io):
    self.prologue = prologue
    self.io = io

  def stream(self, tick=0):
    demo_io = d_io.construct(self.io)

    meta, recv_tables, string_tables, game_event_list = self.prologue

    cb = meta.class_bits
    rt = recv_tables
    st = copy.deepcopy(string_tables)
    mm = o_am.construct()

    full_packets, stragglers = [], []

    if tick:
      iter_bootstrap = iter(demo_io)

      try:
        while True:
          p, m = next(iter_bootstrap)

          if p.kind == pb_d.DEM_FullPacket:
            full_packets.append((p, m))
            stragglers = []
          else:
            stragglers.append((p, m))

          if p.tick > tick:
            break
      except StopIteration:
        return None

    for peek, message in full_packets:
      full_packet = d_io.parse(peek.kind, peek.compressed, message)

      for table in full_packet.string_table.tables:
        assert not table.items_clientside

        entries = [(_i, e.str, e.data) for _i, e in enumerate(table.items)]
        st[table.table_name].update_all(entries)

    world = w.construct(rt)

    if full_packets:
      peek, message = full_packets[-1]
      pbmsg = d_io.parse(peek.kind, peek.compressed, message)
      packet = i.construct(p_io.construct(pbmsg.packet.data))

      peek, message = packet.find(pb_n.svc_PacketEntities)
      pe = p_io.parse(peek.kind, message)
      ct = pe.updated_entries
      bs = b_io.construct(pe.entity_data)
      unpacker = u_ent.unpack(bs, -1, ct, False, cb, world)

      for index, mode, (cls, serial, diff) in unpacker:
        data = st['instancebaseline'].get(cls)[1]
        bs = b_io.construct(data)
        unpacker = u_ent.unpack(bs, -1, 1, False, cb, world)
        state = dict(unpacker.unpack_baseline(rt[cls]))
        state.update(diff)

        try:
          world.create(cls, index, serial, state)
        except AssertionError, e:
          print e

    return Stream(self.io, cb, rt, st, mm, world, stragglers=stragglers)

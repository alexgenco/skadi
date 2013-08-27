from skadi import index as i
from skadi.protoc import demo_pb2 as pb_d


test_full_packet = lambda p: p.kind == pb_d.DEM_FullPacket


def construct(io, tick=0):
  iter_entries = iter(io)

  def advance():
    p, m = next(iter_entries)
    if p.tick > tick:
      raise StopIteration()
    return (p, m)

  entries = [(p, m) for p, m in iter(advance, None) if test_full_packet(p)]

  return Index(entries)


class Index(i.Index):
  def __init__(self, entries):
    super(Index, self).__init__(entries)

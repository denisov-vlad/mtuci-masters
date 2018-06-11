import os

def encode(s):
    if isinstance(s, bytes):
        return s.decode('utf-8', 'ignore')
    else:
        return str(s)


path_joiner = os.path.join
path_basename = os.path.basename
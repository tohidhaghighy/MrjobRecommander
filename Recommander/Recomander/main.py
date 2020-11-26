import pickle
from base64 import b64decode

with open("out.txt") as fp:
    a = fp.read()

b = a.encode("utf8")
c = b64decode(b)
d = pickle.loads(c)
print(d)
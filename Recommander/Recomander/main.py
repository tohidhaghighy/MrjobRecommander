import pickle
from base64 import b64decode
import numpy as np

with open("out.txt") as fp:
    a = fp.read()

b = a.encode("utf8")
c = b64decode(b)
d = pickle.loads(c)

np.fill_diagonal(d,0)

print(d.argmax(axis=1))

similar_users = {}
for i, row in enumerate(d, start=1):
    similar_users[i] = []
    for j, v in enumerate(row, start=1):
        if v>0.5:
            similar_users[i].append(j)

print(similar_users[1])






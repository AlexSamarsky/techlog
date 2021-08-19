def g():
    yield 1

d = g()
next(d)
u = next(d, None)
print(u)
for i in d:
    print(i)
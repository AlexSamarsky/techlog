with open('test.log', encoding="utf-8") as f:
    c = 0
    cnt = 0
    # for line in f:
    line = f.readline()
    cnt += 1
    c += len(line)
    print(f'tell: {f.tell()}')
    print(f'test utf-8: {len(line.encode("utf-8"))}')
    print(f'test utf-16: {len(line.encode("utf-16"))}')
    print(f'test bytes: {len(bytes(line, "utf8"))}')
    # print('begin')
    
    # for char in line:
    #     print (f'{char}')
    #     # s = ff.readline()
    #     # print(f'len line: {len(line)}')
    #     # print(f's: {s}')
    #     # if cnt > 5: break
    # print('end')
print('')
print('')
print(c)
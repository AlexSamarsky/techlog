# from Timer import Timer

# def read(file_name, byte):
#     t = Timer(f'timer read 20 with {byte} bytes')
#     t.start()
#     with open(file_name, 'r', encoding="utf-8-sig") as f:
#         for i in range(20):
#             s = f.read(byte)
#         print(f'current file position: {f.tell()}')
#     t.stop()
#     print(t)


# def main():
#     str = '//app-bill-nord/logz_full/Logs_full/rphost_500/21082009.log'
#     read(str, 1000)
#     read(str, 10000)
#     read(str, 100000)
#     read(str, 1000000)
#     read(str, 10000000)
#     # read(str, 100_000_000)

# if __name__ == '__main__':
#     main()

import numpy as np


class TTT():
    # s = '2fdsa\n2rewq\n3vczx\niuyt\nlkjh'

    def q():
        print('fdsa')

    def v(self, a, b):
        # q()
        # print(b[a])
        print(a)
        print(b)
        return b

    # arr = np.fromstring(s, sep='\n')
    def method(self):
        l = ['fdsa', 'rewq', 'vcxz', 'uytr']
        t = np.r_[0:len(l):1]
        print(t)
        arr = np.array(l)
        # print(arr)

        vect = np.vectorize(self.v)
        fff = vect(a=l, b=l)
        print(fff)

fdsa = TTT()
fff = fdsa.method()
# print(fff)
s = '2fdsa\n2rewq\n3vczx\niuyt\nlkjh'
aaa = np.char.split(s, sep='\n')
print(aaa)
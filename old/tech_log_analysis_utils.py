import csv
import sys
import ctypes as ct


class TechLogAnalysisUtils:

    @staticmethod
    def set_field_size_limit():
        csv.field_size_limit(int(ct.c_ulong(-1).value // 2))
        # max_int = sys.maxsize
        # while True:
        #     # decrease the maxInt value by factor 10
        #     # as long as the OverflowError occurs.
        #     try:
        #         csv.field_size_limit(max_int)
        #         break
        #     except OverflowError:
        #         max_int = int(max_int / 10)

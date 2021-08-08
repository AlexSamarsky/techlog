import re
from TechLogCsv import TechLogCsv


class TechLogCsvNormalization(TechLogCsv):
    
    _pattern_table = re.compile(r"([^\w]tt)\d+", flags=re.ASCII)
    _pattern_table1 = re.compile(r"\w+-\w+-\w+-\w+-\w+", flags=re.ASCII)
    _pattern_table2 = re.compile(r"\b_Q_\d{3}_F_\d{3}", flags=re.ASCII)
    _pattern_table3 = re.compile(r"([^V]T)\d+([^\d])", flags=re.ASCII)
    _pattern_table4 = re.compile(r"p_0.*", flags=re.ASCII)
    _pattern_table5 = re.compile(r"\(\d+\)", flags=re.ASCII)

    def field_value_process(self, field_name, field_value):
        if field_name == 'sql':
            field_value = self._pattern_table.sub(r'\1', field_value)
            field_value = self._pattern_table1.sub(r'GUID', field_value)
            field_value = self._pattern_table2.sub(r'QField', field_value)
            field_value = self._pattern_table3.sub(r'\1\2', field_value)
            field_value = self._pattern_table4.sub(r'', field_value)
            field_value = self._pattern_table5.sub(r'({NUM})', field_value)
        return field_value
    
if __name__ == '__main__':
    file_in_name = 'logs/'
    # file_in_name = 'logs_test/test.log'
    file_out_name = 'logs_test/test_out.csv'
    file_field_filter_name = 'fields.txt'
    tlc = TechLogCsvNormalization(file_in_name, file_out_name, file_field_filter_name)
    tlc.main_process()
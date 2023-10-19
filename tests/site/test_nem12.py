from bua.site.action.nem12 import NEM12ContentGenerator
from datetime import datetime

class TestCase:

    def test_nem12_file_generation_peak_offpeak(self):
        file_date_time = '20231001'
        update_date_time = '20231001'
        identifier = '1234567890'
        generator = NEM12ContentGenerator(
            file_date_time=file_date_time, update_date_time=update_date_time, identifier=identifier
        )
        values = {f'value_{n:02}': 0.1 * n for n in range(1, 49)}
        record1 = {
            'nmi': identifier,
            'read_date': datetime.strptime('2023-09-27', '%Y-%m-%d'),
            'suffix_id': 'E1', 'register_id': 'E1',
            'serial': '123456', 'unit_of_measure': 'KWH', 'segment_identifier': 'SA|BUS|PRIMARY',
            'day_type': 'WEEKDAY', 'scalar': 1.1, 'time_set_id': 123, 'time_class_name': 'OFFPEAK',
            'start_row': 0, 'end_row': 24, **values
        }
        record2 = {
            'nmi': identifier,
            'read_date': datetime.strptime('2023-09-27', '%Y-%m-%d'),
            'suffix_id': 'E1', 'register_id': 'E1',
            'serial': '123456', 'unit_of_measure': 'KWH', 'segment_identifier': 'SA|BUS|PRIMARY',
            'day_type': 'WEEKDAY', 'scalar': 1.2, 'time_set_id': 123, 'time_class_name': 'PEAK',
            'start_row': 24, 'end_row': 48, **values
        }
        records = [record1, record2]
        output = generator.generate_nem12_file_content(records).getvalue().splitlines()
        assert len(output) == 4
        assert output[0] == '100,NEM12,20231001,BUA,BUA'
        assert output[1] == '200,1234567890,E1,E1,E1,E1,123456,KWH,30,'
        assert output[2] == '300,20230927,0.110000,0.220000,0.330000,0.440000,0.550000,0.660000,0.770000,0.880000,0.990000,1.100000,1.210000,1.320000,1.430000,1.540000,1.650000,1.760000,1.870000,1.980000,2.090000,2.200000,2.310000,2.420000,2.530000,2.640000,3.000000,3.120000,3.240000,3.360000,3.480000,3.600000,3.720000,3.840000,3.960000,4.080000,4.200000,4.320000,4.440000,4.560000,4.680000,4.800000,4.920000,5.040000,5.160000,5.280000,5.400000,5.520000,5.640000,5.760000,AB,,,20231001,'
        assert output[3] == '900'

    def test_nem12_file_generation_no_time_of_day(self):
        file_date_time = '20231001'
        update_date_time = '20231001'
        identifier = '1234567890'
        generator = NEM12ContentGenerator(
            file_date_time=file_date_time, update_date_time=update_date_time, identifier=identifier
        )
        values = {f'value_{n:02}': 0.1 * n for n in range(1, 49)}
        record1 = {
            'nmi': identifier,
            'read_date': datetime.strptime('2023-09-27', '%Y-%m-%d'),
            'suffix_id': 'E1', 'register_id': 'E1',
            'serial': '123456', 'unit_of_measure': 'KWH', 'segment_identifier': 'SA|BUS|PRIMARY',
            'day_type': 'WEEKDAY', 'scalar': None, 'time_set_id': None, 'time_class_name': None,
            'start_row': None, 'end_row': None, **values
        }
        records = [record1]
        output = generator.generate_nem12_file_content(records).getvalue().splitlines()
        assert len(output) == 4
        assert output[0] == '100,NEM12,20231001,BUA,BUA'
        assert output[1] == '200,1234567890,E1,E1,E1,E1,123456,KWH,30,'
        assert output[2] == '300,20230927,0.100000,0.200000,0.300000,0.400000,0.500000,0.600000,0.700000,0.800000,0.900000,1.000000,1.100000,1.200000,1.300000,1.400000,1.500000,1.600000,1.700000,1.800000,1.900000,2.000000,2.100000,2.200000,2.300000,2.400000,2.500000,2.600000,2.700000,2.800000,2.900000,3.000000,3.100000,3.200000,3.300000,3.400000,3.500000,3.600000,3.700000,3.800000,3.900000,4.000000,4.100000,4.200000,4.300000,4.400000,4.500000,4.600000,4.700000,4.800000,AB,,,20231001,'
        assert output[3] == '900'

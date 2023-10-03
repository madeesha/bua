import struct
import traceback
from decimal import Decimal

import pytest
from boto3.dynamodb.types import Binary

from bua.site.action.sitedata import SiteData
from bua.site.action.sitesegment import SiteSegment
from bua.site.handler.initiate import BUASiteInitiateHandler


class TestClass:

    @pytest.mark.parametrize('source,scale,expected', [
        ([Decimal(10), Decimal(20)], 4, [Decimal(5), Decimal(5), Decimal(10), Decimal(10)]),
        ([Decimal(10), Decimal(20), Decimal(30), Decimal(40)], 2, [Decimal(30), Decimal(70)]),
        ([Decimal(10), Decimal(20), Decimal(30), Decimal(40)], 4, [Decimal(10), Decimal(20), Decimal(30), Decimal(40)])
    ])
    def test_standardise_intervals(self, source, scale, expected):
        assert SiteData._standardise_intervals(source, scale) == expected

    @pytest.mark.parametrize('source,scale,expected', [
        (['A', 'E'], 4, ['A', 'A', 'E', 'E']),
        (['A', 'E', 'A', 'A'], 2, ['E', 'A']),
        (['A', 'A', 'E', 'E'], 4, ['A', 'A', 'E', 'E'])
    ])
    def test_standardise_qualities(self, source, scale, expected):
        assert SiteData._standardise_qualities(source, scale) == expected

    @pytest.mark.parametrize('source,expected', [
        (['A', 'A'], 'A'),
        (['A', 'E'], 'E'),
        (['A', 'F'], 'F'),
        (['A', 'S'], 'S'),
        (['S', 'A'], 'S'),
        (['S', 'E'], 'E'),
        (['S', 'F'], 'S'),
        (['S', 'S'], 'S'),
        (['F', 'A'], 'F'),
        (['F', 'E'], 'E'),
        (['F', 'F'], 'F'),
        (['F', 'S'], 'S'),
        (['E', 'A'], 'E'),
        (['E', 'E'], 'E'),
        (['E', 'F'], 'E'),
        (['E', 'S'], 'E'),
    ])
    def test_max_qualities(self, source, expected):
        assert SiteData._max(source) == expected

    @pytest.mark.parametrize('source,expected', [
        ({'DI': 'I'}, 'I'),
        ({'SUFX': 'A1'}, 'I'),
        ({'SUFX': 'B1'}, 'I'),
        ({'SUFX': 'C1'}, 'I'),
        ({'SUFX': 'J1'}, 'I'),
        ({'SUFX': 'K1'}, 'I'),
        ({'SUFX': 'L1'}, 'I'),
        ({'SUFX': 'E1'}, 'E'),
        ({'SUFX': 'Q1'}, 'E'),
    ])
    def test_derive_direction_indicator(self, source, expected):
        assert SiteData._derive_direction_indicator(source) == expected

    @pytest.mark.parametrize('source,expected', [
        ({'ILEN': '360', 'QM': '["A"]', 'SI': '[1]', 'EI': '[4]'}, ['A', 'A', 'A', 'A']),
        ({'ILEN': '360', 'QM': '["A", "E"]', 'SI': '[1,3]', 'EI': '[2,4]'}, ['A', 'A', 'E', 'E'])
    ])
    def test_derive_interval_qualities(self, source, expected):
        assert SiteData._derive_interval_qualities(source) == expected

    @pytest.mark.parametrize('source,expected', [
        ({'ILEN': '360', 'DATA': Binary(struct.pack('<HHHH', 123, 234, 345, 456)), 'PS': '2,2,2,2'},
         [Decimal('1.23'), Decimal('2.34'), Decimal('3.45'), Decimal('4.56')])
    ])
    def test_decode_interval_data(self, source, expected):
        assert SiteData._decode_interval_data(source) == expected

    @pytest.mark.parametrize('source,expected', [
        ({'Items': [
            {
                'VER': 'NEM12',
                'IDENT': '1234567890',
                'SUFX': 'E1',
                'IDATE': '20230101',
                'ILEN': '720',
                'DATA': Binary(struct.pack('<HH', 480, 960)),
                'PS': '1,1',
                'QM': '["A", "E"]',
                'SI': '[1,2]',
                'EI': '[1,2]',
                'CFG': 'E1B1Q1K1',
                'UOM': 'KWH',
                'FDTE': '20230101020305',
                'UPDT': '20230101020304',
                'SN': 'SERIAL123',
                'REG': '1',
                'MDM': 'N1',
            }
        ]}, {
            '1234567890|E1|20230101': {
                'NMI': '1234567890',
                'SFX': 'E1',
                'CFG': 'E1B1Q1K1',
                'UOM': 'KWH',
                'IDT': '20230101',
                'FDT': '20230101020305',
                'UDT': '20230101020304',
                'MDT': None,
                'SER': 'SERIAL123',
                'REG': '1',
                'MDM': 'N1',
                'DIR': 'E',
                'TOT': Decimal('144'),
                'CNT': 48,
                'TEST': Decimal('96'),
                'CEST': 24,
                'TACT': Decimal('48'),
                'CACT': 24,
                'TSUB': Decimal('0'),
                'CSUB': 0,
                'TFIN': Decimal('0'),
                'CFIN': 0,
                'VAL': [Decimal('2')] * 24 + [Decimal('4')] * 24,
                'QUA': ['A'] * 24 + ['E'] * 24,
            }
        })
    ])
    def test_process_query_response(self, source, expected):
        records = dict()
        SiteData._process_query_response(source, records)
        assert records == expected

    @pytest.mark.parametrize('nmi,res_bus,jurisdiction,tni,stream_types,start_inclusive,end_exclusive,records,count', [
        ('1234567890', 'RES', 'QLD', 'QSPN', {'B1': 'SOLAR', 'E1': 'PRIMARY'}, '2023-01-01', '2023-01-01', [
            {'IDT': '20230101', 'SFX': 'E1', 'CFG': 'B1E1', 'DIR': 'E',
             'TOT': '48', 'CNT': '48',
             'TEST': '0', 'CEST': '0',
             'TACT': '48', 'CACT': '0',
             'TSUB': '0', 'CSUB': '0',
             'TFIN': '0', 'CFIN': '0',
             'VAL': ['1']*48, 'QUA': ['A']*48,
             'UOM': 'KWH', 'FDT': '202301010203', 'UDT': '20230101020304', 'MDT': None, 'SER': 'SERIAL1', 'REG': '001',
             'MDM': 'N1'}
        ], 5),
        ('1234567890', 'RES', 'QLD', 'QSPN', {'B1': 'SOLAR', 'E1': 'PRIMARY'}, '2023-01-01', '2023-01-01', [], 4),
    ])
    def test_insert(self, nmi, res_bus, jurisdiction, tni, stream_types, start_inclusive, end_exclusive, records, count):
        table = {}
        queue = {}
        conn = Database()
        site = SiteData(table, queue, conn)
        site.insert_site_data('Utility', '2023-06-01 00:00:00', nmi, res_bus, jurisdiction, tni, stream_types,
                              start_inclusive, end_exclusive, records)
        assert len(conn.executions) == count
        assert conn.commits == [True]

    @pytest.mark.parametrize('identifier_type,jurisdiction_name,tni_name,res_bus,stream_type,avg_sum,incl_est,aggregator,filter,identifier', [
        ('SegmentTNIAvgIncl', 'VIC', 'NSLP', 'RES', 'PRIMARY', 'Average', True, 'AVG', None, 'VIC|NSLP|RES|PRIMARY'),
        ('SegmentTNISumIncl', 'VIC', 'NSLP', 'RES', 'PRIMARY', 'Sum', True, 'SUM', None, 'VIC|NSLP|RES|PRIMARY'),
        ('SegmentTNIAvgExcl', 'VIC', 'NSLP', 'RES', 'PRIMARY', 'Average', False, 'AVG', 'IF(SUBSTR(quality', 'VIC|NSLP|RES|PRIMARY'),
        ('SegmentTNISumExcl', 'VIC', 'NSLP', 'RES', 'PRIMARY', 'Sum', False, 'SUM', 'IF(SUBSTR(quality', 'VIC|NSLP|RES|PRIMARY'),
        ('SegmentJurisdictionAvgIncl', 'VIC', None, 'RES', 'PRIMARY', 'Average', True, 'AVG', None, 'VIC|RES|PRIMARY'),
        ('SegmentJurisdictionSumIncl', 'VIC', None, 'RES', 'PRIMARY', 'Sum', True, 'SUM', None, 'VIC|RES|PRIMARY'),
        ('SegmentJurisdictionAvgExcl', 'VIC', None, 'RES', 'PRIMARY', 'Average', False, 'AVG', 'IF(SUBSTR(quality', 'VIC|RES|PRIMARY'),
        ('SegmentJurisdictionSumExcl', 'VIC', None, 'RES', 'PRIMARY', 'Sum', False, 'SUM', 'IF(SUBSTR(quality', 'VIC|RES|PRIMARY'),
    ])
    def test_calculate_segment(self, identifier_type, jurisdiction_name, tni_name, res_bus, stream_type,
                                                avg_sum, incl_est, aggregator, filter, identifier):
        table = {}
        queue = {}
        conn = Database(rowcount=10)
        site = SiteSegment(table, queue, conn)
        printer = Printer()
        site.log = printer.print
        site.calculate_profile_segment(
            identifier_type=identifier_type,
            run_date='2023-06-01 00:00:00',
            source_date='2023-06-01 00:00:00',
            jurisdiction_name=jurisdiction_name,
            tni_name=tni_name,
            res_bus=res_bus,
            stream_type=stream_type,
            interval_date='2022-06-01',
            avg_sum=avg_sum,
            incl_est=incl_est
        )
        assert len(conn.executions) == 4
        assert aggregator in conn.executions[2][0]
        if filter is not None:
            assert filter in conn.executions[2][0]
        assert conn.commits == [True]
        assert len(printer.prints) == 1
        assert printer.prints[0][0] == f'Imported 10 records for segment {identifier} on 2022-06-01'

    def test_validate_site_data(self):
        table = {}
        queue = {}
        conn = Database(rowcount=10)
        site = SiteData(table, queue, conn, check_aggread=True, check_nem=True)
        printer = Printer()
        site.log = printer.print
        site.validate_site_data('1234567890', 'Validate', '2023-06-01 00:00:00', '2023-06-01 00:00:00', '2022-06-01', '2023-06-01')
        assert len(conn.executions) == 5
        assert conn.commits == [True]
        assert len(printer.prints) == 1
        assert printer.prints[0][0] == '20 variances identified for nmi 1234567890'

    def test_site_initiate_handler(self):
        sqs_client = {}
        ddb_meterdata_table = {}
        ddb_bua_table = {}
        data_queue = {}
        segment_queue = {}
        conn = Database(rowcount=10)
        handler = BUASiteInitiateHandler(
            sqs_client=sqs_client, ddb_meterdata_table=ddb_meterdata_table, ddb_bua_table=ddb_bua_table,
            data_queue=data_queue, segment_queue=segment_queue, conn=conn
        )
        event = {
            'run_type': 'SegmentJurisdictionTotal',
            'start_inclusive': '2022-05-01',
            'end_exclusive': '2023-05-01'
        }
        handler.handle_request(event)

    @pytest.mark.parametrize('nmi_suffix, stream_types, mdm_config, result', [
        ('E1', {'E1': 'PRIMARY'}, 'E1', 'PRIMARY'),
        ('E1', {'E1': 'CONTROL'}, 'E2E1', 'CONTROL'),
        ('B1', {'B1': 'SOLAR'}, 'B1E1', 'SOLAR'),
        ('B1', {'E1': 'PRIMARY'}, 'B1E1', 'SOLAR'),
        ('E1', {'B1': 'SOLAR'}, 'B1E1', 'PRIMARY'),
        ('E2', {'E1': 'PRIMARY'}, 'E1E2', 'UNKNOWN')
    ])
    def test_derive_stream_type(self, nmi_suffix, stream_types, mdm_config, result):
        assert SiteData._derive_stream_type(nmi_suffix, stream_types, mdm_config) == result


class Printer:
    def __init__(self):
        self.prints = []

    def print(self, *args, **kwargs):
        formatted = ' '.join([str(v) for v in args])
        self.prints.append((formatted, args, kwargs))
        print(*args, **kwargs)


class Database:
    def __init__(self, rowcount=0):
        self.executions = []
        self.commits = []
        self.unbuffered_result = []
        self.rowcount = rowcount

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def execute(self, sql, args=None):
        self.executions.append((sql, args))
        try:
            if args is not None:
                sql % tuple(args)
        except TypeError as te:
            traceback.print_exception(te)
            print(sql)
            print(args)
            sql_arg_count = (len(sql) - len(sql.replace('%s', '')))//2
            print('Required', sql_arg_count, 'args but received ', len(args) if args is not None else 0, 'arguments')
            raise

    def commit(self):
        self.commits.append(True)

    def fetchall_unbuffered(self):
        return self.unbuffered_result

import decimal
import json
import struct
import traceback
from typing import Optional

from boto3.dynamodb.conditions import Key
from pymysql import Connection

from bua.site.action import Action


class SiteData(Action):

    def __init__(self, ddb_meterdata_table, queue, conn: Connection,
                 debug=False, batch_size=10, check_nem=True, check_aggread=False):
        super().__init__(queue, conn, debug)
        self.ddb_meterdata_table = ddb_meterdata_table
        self.batch_size = batch_size
        self.check_nem = check_nem
        self.check_aggread = check_aggread

    def initiate_site_data_processing(self, run_type, run_date, today, start_inclusive, end_exclusive, source_date,
                                      limit=1000000000):
        """Initiate the extraction or validation of site data"""
        bodies = []
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    "DELETE FROM UtilityProfileSummary "
                    "WHERE run_date = %s "
                    "AND identifier_type = %s "
                    "AND interval_date = %s",
                    (run_date, run_type, start_inclusive)
                )
                if run_type == 'Validate':
                    cur.execute(
                        "DELETE FROM UtilityProfileVariance "
                        "WHERE run_date = %s "
                        "AND identifier_type = %s "
                        "AND interval_date >= %s "
                        "AND interval_date < %s",
                        (run_date, run_type, start_inclusive, end_exclusive)
                    )
                if run_type == 'Utility':
                    cur.execute(
                        "DELETE FROM UtilityProfile "
                        "WHERE run_date = %s "
                        "AND identifier_type = %s "
                        "AND interval_date >= %s "
                        "AND interval_date < %s",
                        (run_date, run_type, start_inclusive, end_exclusive)
                    )
                cur.execute(
                    "CALL bua_list_profile_registers(%s, %s, %s, %s)",
                    (start_inclusive, end_exclusive, today, run_date)
                )
                body = None
                total = 0
                for record in cur.fetchall_unbuffered():
                    nmi = record['nmi']
                    res_bus = record['res_bus']
                    jurisdiction = record['jurisdiction']
                    tni = record['tni']
                    nmi_suffix = record['nmi_suffix']
                    stream_type = record['stream_type']
                    if body is None or body['nmi'] != nmi:
                        if total >= limit:
                            break
                        if body is not None:
                            bodies.append(body)
                            self.send_if_needed(bodies, batch_size=self.batch_size)
                        body = {
                            'run_type': run_type,
                            'run_date': run_date,
                            'source_date': source_date,
                            'nmi': nmi,
                            'res_bus': res_bus,
                            'jurisdiction': jurisdiction,
                            'tni': tni,
                            'stream_types': {
                                nmi_suffix: stream_type
                            },
                            'start_inclusive': start_inclusive,
                            'end_exclusive': end_exclusive
                        }
                        total += 1
                    else:
                        body['stream_types'][nmi_suffix] = stream_type
                if body is not None:
                    bodies.append(body)
                self.send_if_needed(bodies, force=True, batch_size=self.batch_size)
                cur.execute(
                    "INSERT INTO UtilityProfileLog (run_date, run_type, source_date, total_entries) "
                    "VALUES (%s, %s, %s, %s)",
                    (run_date, run_type, source_date, total)
                )
                run_method = "extract" if run_type == 'Utility' else "validate"
                self.log(f'{total} sites to {run_method} profile data for (limit {limit})')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def query_site_data(self, nmi: str, start_inclusive: str, end_exclusive: str):
        """Query the specified site and return the latest reads"""
        records = dict()
        sk_start = start_inclusive.replace('-','')
        sk_end = end_exclusive.replace('-','')
        for pk in [f'NEM|DATA|NMI|{nmi}|ACT', f'NEM|DATA|NMI|{nmi}|EST']:
            key_expr = Key('PK').eq(pk) & Key('SK').between(sk_start, sk_end)
            response = self.ddb_meterdata_table.query(KeyConditionExpression=key_expr)
            self._process_query_response(response, records)
            while 'LastEvaluatedKey' in response:
                response = self.ddb_meterdata_table.query(KeyConditionExpression=key_expr,
                                                          ExclusiveStartKey=response['LastEvaluatedKey'])
                self._process_query_response(response, records)
        return list(records.values())

    @staticmethod
    def _process_query_response(response, records):
        """Process a dynamodb query response to determine a set of the latest version of data"""
        if 'Items' in response:
            for item in response['Items']:
                try:
                    if SiteData._is_valid_item(item):
                        intervals = SiteData._standardise_intervals(SiteData._decode_interval_data(item), 48)
                        qualities = SiteData._standardise_qualities(SiteData._derive_interval_qualities(item), 48)
                        direction = SiteData._derive_direction_indicator(item)
                        total_value = sum(intervals)
                        count_value = len(intervals)
                        estimates = [value for index, value in enumerate(intervals) if qualities[index].startswith('E')]
                        total_estimates = sum(estimates)
                        count_estimates = len(estimates)
                        actuals = [value for index, value in enumerate(intervals) if qualities[index].startswith('A')]
                        total_actuals = sum(actuals)
                        count_actuals = len(actuals)
                        substitutes = [value for index, value in enumerate(intervals) if qualities[index].startswith('S')]
                        total_substitutes = sum(substitutes)
                        count_substitutes = len(substitutes)
                        finals = [value for index, value in enumerate(intervals) if qualities[index].startswith('F')]
                        total_finals = sum(finals)
                        count_finals = len(finals)
                        nmi = item['IDENT']
                        sfx = item['SUFX']
                        idt = item['IDATE']
                        record = {
                            'NMI': nmi,                # NMI
                            'SFX': sfx,                # NMI suffix
                            'CFG': item.get('CFG'),    # MDM configuration
                            'UOM': item['UOM'],        # Unit of measure
                            'IDT': idt,                # Interval date
                            'FDT': item['FDTE'],       # File date
                            'UDT': item['UPDT'],       # MDP update time
                            'MDT': item.get('MSAT'),   # MSATS time
                            'SER': item.get('SN'),     # serial number
                            'REG': item.get('REG'),    # register
                            'MDM': item.get('MDM'),    # MDM data stream identifier
                            'DIR': direction,          # Direction
                            'TOT': total_value,        # total reads
                            'CNT': count_value,        # count reads
                            'TEST': total_estimates,   # total estimates
                            'CEST': count_estimates,   # count estimates
                            'TACT': total_actuals,     # total actuals
                            'CACT': count_actuals,     # count actuals
                            'TSUB': total_substitutes, # total substitutes
                            'CSUB': count_substitutes, # count substitutes
                            'TFIN': total_finals,      # total finals
                            'CFIN': count_finals,      # count finals
                            'VAL': intervals,          # meter read interval values
                            'QUA': qualities,          # meter read qualities
                        }
                        key = f'{nmi}|{sfx}|{idt}'
                        if key not in records:
                            records[key] = record
                        elif records[key]['UDT'] < record['UDT']:
                            records[key] = record
                        elif records[key]['UDT'] == record['UDT'] and records[key]['FDT'] < record['FDT']:
                            records[key] = record
                except Exception as ex:
                    traceback.print_exception(ex)
                    print(item)
                    raise

    @staticmethod
    def _is_valid_item(item):
        if item.get('VER') != 'NEM12':
            return False
        if item.get('FROM') == 'CORE':
            return False
        if item.get('TO') == 'CORE':
            return False
        if 'IDENT' not in item:
            return False
        return True

    @staticmethod
    def _standardise_intervals(intervals, count):
        """Standardise the intervals by upscaling or downscaling"""
        control = sum(intervals)
        if len(intervals) > count:
            multiplier = int(len(intervals) / count)
            intervals = [sum(intervals[index*multiplier:(index+1)*multiplier]) for index in range(count)]
        elif len(intervals) < count:
            divider = int(count / len(intervals))
            divisor = decimal.Decimal(divider)
            intervals = [intervals[index//divider] / divisor for index in range(count)]
        validate = sum(intervals)
        if control != validate:
            raise Exception(f'Control: {control} does not match Validate: {validate}')
        return intervals

    @staticmethod
    def _decode_interval_data(item):
        """decode the dynamodb encoded interval data"""
        interval_length = int(item['ILEN'])
        intervals = int(1440 / interval_length)
        data = item['DATA'].value
        bytes_per_interval = int(len(data) / intervals)
        scale = int(item['PS'].split(',')[1])
        if bytes_per_interval == 1:
            pack_format = '<B'
        elif bytes_per_interval == 2:
            pack_format = '<H'
        elif bytes_per_interval == 4:
            pack_format = '<I'
        else:
            pack_format = '<Q'
        result = []
        for num in struct.iter_unpack(pack_format, data):
            number = decimal.Decimal(num[0])
            if scale > 0:
                for n in range(scale):
                    number = number / decimal.Decimal(10)
            result.append(number)
        return result

    @staticmethod
    def _standardise_qualities(qualities, count):
        """Standardise the qualities by compressing to the lowest quality or expanding qualities as needed"""
        if len(qualities) > count:
            multiplier = int(len(qualities) / count)
            return [SiteData._max(qualities[index*multiplier:(index+1)*multiplier]) for index in range(count)]
        elif len(qualities) < count:
            divider = int(count / len(qualities))
            qualities = [qualities[index//divider] for index in range(count)]
        return qualities

    @staticmethod
    def _max(qualities):
        """determine the lowest quality from a set of qualities"""
        if 'E' in qualities:
            return 'E'
        if 'S' in qualities:
            return 'S'
        if 'F' in qualities:
            return 'F'
        return qualities[0]

    @staticmethod
    def _derive_interval_qualities(item):
        """derive the dynamodb encoded interval qualities"""
        interval_length = int(item['ILEN'])
        intervals = int(1440 / interval_length)
        qualities = []
        for i in range(intervals):
            qualities.append(None)
        qms = json.loads(item['QM'])
        sis = json.loads(item['SI'])
        eis = json.loads(item['EI'])
        for i in range(len(sis)):
            si = sis[i]
            ei = eis[i]
            qm = qms[i]
            for iv in range(si, ei+1):
                qualities[iv-1] = qm
        return qualities

    @staticmethod
    def _derive_direction_indicator(item):
        """derive the direction indicator"""
        if 'DI' in item:
            return item['DI']
        suffix = item['SUFX']
        if suffix[0:1] in ['A', 'B', 'C', 'J', 'K', 'L']:
            return 'I'
        return 'E'

    def insert_site_data(self, run_type, run_date, nmi, res_bus, jurisdiction, tni, stream_types,
                         start_inclusive, end_exclusive, records):
        """Insert site data records into UtilityProfile"""
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    "DELETE FROM UtilityProfile "
                    "WHERE run_date = %s "
                    "AND identifier_type = %s "
                    "AND identifier = %s "
                    "AND interval_date >= %s "
                    "AND interval_date < %s",
                    (run_date, run_type, nmi, start_inclusive, end_exclusive)
                )
                cur.execute(
                    "DELETE FROM UtilityProfileSummary "
                    "WHERE run_date = %s "
                    "AND identifier_type = %s "
                    "AND identifier = %s "
                    "AND interval_date = %s",
                    (run_date, run_type, nmi, start_inclusive)
                )
                cur.execute(
                    "DELETE FROM UtilityProfileVariance "
                    "WHERE run_date = %s "
                    "AND identifier_type = %s "
                    "AND identifier = %s "
                    "AND interval_date >= %s "
                    "AND interval_date < %s",
                    (run_date, run_type, nmi, start_inclusive, end_exclusive)
                )
                sql = None
                sql_args = []
                for record in records:
                    read_date = record['IDT']
                    nmi_suffix = record['SFX']
                    mdm_config = record['CFG']
                    stream_type = SiteData._derive_stream_type(nmi_suffix, stream_types, mdm_config)
                    fields = ['run_date', 'identifier_type', 'identifier', 'interval_date', 'nmi_suffix', 'res_bus',
                              'jurisdiction_name', 'tni_name', 'stream_type', 'mdm_config', 'direction_indicator',
                              'total_interval_value', 'total_interval_count',
                              'total_estimate_value', 'total_estimate_count',
                              'total_actual_value', 'total_actual_count',
                              'total_substitute_value', 'total_substitute_count',
                              'total_final_value', 'total_final_count']
                    args = [run_date, run_type, nmi, read_date, nmi_suffix, res_bus, jurisdiction, tni, stream_type,
                            record['CFG'], record['DIR'],
                            str(record['TOT']), str(record['CNT']),
                            str(record['TEST']), str(record['CEST']),
                            str(record['TACT']), str(record['CACT']),
                            str(record['TSUB']), str(record['CSUB']),
                            str(record['TFIN']), str(record['CFIN']),
                            ]
                    for index in range(len(record['VAL'])):
                        fields.append(f'value_{index+1:02d}')
                        args.append(str(record['VAL'][index]))
                        fields.append(f'quality_{index+1:02d}')
                        args.append(record['QUA'][index])
                    fields.extend(['unit_of_measure', 'file_date_time', 'mdp_update_date_time',
                                   'msats_update_date_time',
                                   'meter_serial', 'register_identifier', 'mdm_data_stream_identifier'])
                    args.append(record['UOM'])
                    args.append(f"{record['FDT']}00")
                    args.append(record['UDT'])
                    args.append(record['MDT'])
                    args.append(record['SER'])
                    args.append(record['REG'])
                    args.append(record['MDM'])
                    columns = ','.join(fields)
                    params = ','.join(['%s'] * len(fields))
                    if sql is None:
                        sql = f"INSERT INTO UtilityProfile ({columns}) VALUES ({params})"
                    else:
                        sql = sql + f",({params})"
                    sql_args.extend(args)
                if sql is not None and len(sql_args) > 0:
                    cur.execute(sql, sql_args)
                Action.record_processing_summary(cur, run_date, run_type, nmi, start_inclusive,
                                                len(records))
                self.log('Imported', len(records), 'records for nmi', nmi)
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    @staticmethod
    def _derive_stream_type(nmi_suffix, stream_types, mdm_config: Optional[str]):
        stream_type = stream_types.get(nmi_suffix)
        if stream_type:
            return stream_type
        if nmi_suffix[0] in 'ABCJKL':
            return 'SOLAR'
        if mdm_config is not None:
            e_types = [mdm_config[index] for index in range(0, len(mdm_config), 2) if mdm_config[index] == 'E']
            if len(e_types) == 1:
                return 'PRIMARY'
        return 'UNKNOWN'

    def validate_site_data(self, nmi, run_type, run_date, source_date, start_inclusive, end_exclusive):
        """Validate site data records against AggRead"""
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    "DELETE FROM UtilityProfileSummary "
                    "WHERE run_date = %s "
                    "AND identifier_type = %s "
                    "AND identifier = %s "
                    "AND interval_date = %s",
                    (run_date, run_type, nmi, start_inclusive)
                )
                cur.execute(
                    "DELETE FROM UtilityProfileVariance "
                    "WHERE run_date = %s "
                    "AND identifier_type = %s "
                    "AND identifier = %s "
                    "AND interval_date >= %s "
                    "AND interval_date < %s",
                    (run_date, 'Utility', nmi, start_inclusive, end_exclusive)
                )
                rows_affected = 0
                # find variance between NEM rows extracted and AggRead rows including missing AggRead rows
                if self.check_nem:
                    cur.execute(
                        """
                        INSERT INTO UtilityProfileVariance
                        (run_date, identifier_type, identifier, interval_date, nmi_suffix, register_identifier,
                         jurisdiction_name, 
                         total_interval_value, shifted_interval_value, agg_read_value,
                         total_actual_value, total_actual_count,
                         total_estimate_value, total_estimate_count,
                         total_substitute_value, total_substitute_count,
                         total_final_value, total_final_count, cr_process)
                        SELECT %s, upcurr.identifier_type, upcurr.identifier, upcurr.interval_date, 
                               upcurr.nmi_suffix, upcurr.register_identifier,
                               upcurr.jurisdiction_name, 
                               upcurr.total_interval_value * IF(upcurr.unit_of_measure='WH',0.001,1), 
                               upcurr.total_interval_value * IF(upcurr.unit_of_measure='WH',0.001,1)
                               + IF(ts.prev_time_shift = 48, IF(upprev.unit_of_measure='WH',0.001,1) * upprev.value_48, 0)
                               + IF(ts.prev_time_shift = 47, IF(upprev.unit_of_measure='WH',0.001,1) * (upprev.value_47 + upprev.value_48), 0)
                               + IF(ts.next_time_shift = 1, IF(upnext.unit_of_measure='WH',0.001,1) * upnext.value_01, 0)
                               - IF(ts.left_time_shift = -1, IF(upcurr.unit_of_measure='WH',0.001,1) * upcurr.value_01, 0)
                               - IF(ts.right_time_shift = -47, IF(upcurr.unit_of_measure='WH',0.001,1) * (upcurr.value_47 + upcurr.value_48), 0)
                               - IF(ts.right_time_shift = -48, IF(upcurr.unit_of_measure='WH',0.001,1) * upcurr.value_48, 0)
                               AS shifted_interval_value,
                           SUM(arcurr.read_value) AS agg_read_value,
                           upcurr.total_actual_value * IF(upcurr.unit_of_measure='WH',0.001,1), upcurr.total_actual_count,
                           upcurr.total_estimate_value * IF(upcurr.unit_of_measure='WH',0.001,1), upcurr.total_estimate_count,
                           upcurr.total_substitute_value * IF(upcurr.unit_of_measure='WH',0.001,1), upcurr.total_substitute_count,
                           upcurr.total_final_value * IF(upcurr.unit_of_measure='WH',0.001,1), upcurr.total_final_count,
                           'BUA-NEM'
                        FROM UtilityProfile upcurr
                        JOIN UtilityProfile upnext 
                        ON upcurr.run_date = upnext.run_date 
                        AND upcurr.identifier_type = upnext.identifier_type 
                        AND upcurr.identifier = upnext.identifier 
                        AND DATE_ADD(upcurr.interval_date, INTERVAL 1 DAY) = upnext.interval_date 
                        AND upcurr.nmi_suffix = upnext.nmi_suffix
                        JOIN UtilityProfile upprev 
                        ON upcurr.run_date = upprev.run_date 
                        AND upcurr.identifier_type = upprev.identifier_type 
                        AND upcurr.identifier = upprev.identifier 
                        AND DATE_SUB(upcurr.interval_date, INTERVAL 1 DAY) = upprev.interval_date 
                        AND upcurr.nmi_suffix = upprev.nmi_suffix
                        JOIN UtilityProfileTimeShift ts 
                        ON ts.jurisdiction_name = upcurr.jurisdiction_name 
                        AND upcurr.interval_date BETWEEN ts.start_date AND ts.end_date
                        JOIN Utility ut ON ut.identifier = upcurr.identifier
                        JOIN AccountUtility au ON au.utility_id = ut.id
                        LEFT JOIN (AggregatedRead arcurr JOIN MeterRegister mr ON mr.id = arcurr.meter_register_id)
                        ON arcurr.account_id = au.account_id 
                        AND arcurr.read_date = upcurr.interval_date 
                        AND arcurr.plan_type_id = 1 -- RETAIL
                        AND arcurr.rev_invoice_id = -1 -- Not Reversed
                        AND arcurr.plan_item_type_id = 2 -- USAGE_RETAIL
                        AND arcurr.generation_type != 'ZEROREADFILLER'
                        AND mr.suffix_id = upcurr.nmi_suffix
                        LEFT JOIN AggregatedRead arnext 
                        ON arnext.account_id = au.account_id 
                        AND arnext.read_date = upcurr.interval_date 
                        AND arnext.plan_type_id = 1 -- RETAIL
                        AND arnext.rev_invoice_id = -1  -- Not Reversed
                        AND arcurr.plan_item_type_id = 2 -- USAGE_RETAIL
                        AND arnext.meter_register_id = arcurr.meter_register_id 
                        AND arnext.time_set_id = arcurr.time_set_id 
                        AND arnext.generation_type != 'ZEROREADFILLER'
                        AND arnext.id > arcurr.id
                        WHERE upcurr.run_date = %s 
                        AND   upcurr.identifier_type = 'Utility'
                        AND   upcurr.identifier = %s
                        AND   upcurr.interval_date >= %s
                        AND   upcurr.interval_date < %s
                        AND arnext.id IS NULL -- there is not a subsequent matching agg read record
                        GROUP BY upcurr.run_date, upcurr.identifier_type, upcurr.identifier, 
                        upcurr.interval_date, upcurr.nmi_suffix, upcurr.register_identifier,
                        upcurr.jurisdiction_name, 
                        upcurr.unit_of_measure, upprev.unit_of_measure, upnext.unit_of_measure,
                        upcurr.total_interval_value, upcurr.total_actual_value, upcurr.total_actual_count,
                        upcurr.total_estimate_value, upcurr.total_estimate_count,
                        upcurr.total_substitute_value, upcurr.total_substitute_count,
                        upcurr.total_final_value, upcurr.total_final_count,
                        upcurr.value_01, upnext.value_01, upcurr.value_02, upnext.value_02, 
                        upcurr.value_48, upcurr.value_47, upprev.value_48, upprev.value_47, 
                        ts.prev_time_shift, ts.next_time_shift, ts.left_time_shift, ts.right_time_shift
                        HAVING COALESCE(agg_read_value,0) != shifted_interval_value
                        """, (run_date, source_date, nmi, start_inclusive, end_exclusive)
                    )
                    rows_affected += max(0, cur.rowcount)
                # find variance where AggRead rows exist but there are no NEM rows
                if self.check_aggread:
                    cur.execute(
                        """
                        INSERT INTO UtilityProfileVariance
                        (run_date, identifier_type, identifier, interval_date, nmi_suffix, register_identifier,
                         jurisdiction_name, agg_read_value, cr_process)
                        SELECT %s, 'Utility', ut.identifier, arcurr.read_date, mr.suffix_id, mr.register_id, ju.name, 
                               SUM(arcurr.read_value), 'BUA-AGG'
                        FROM AggregatedRead arcurr
                        LEFT JOIN AggregatedRead arnext 
                        ON arnext.account_id = arcurr.account_id 
                        AND arnext.read_date = arcurr.read_date 
                        AND arnext.plan_type_id = 1 -- RETAIL
                        AND arnext.rev_invoice_id = -1 -- Not Reversed
                        AND arnext.plan_item_type_id = 2 -- USAGE_RETAIL
                        AND arnext.meter_register_id = arcurr.meter_register_id 
                        AND arnext.time_set_id = arcurr.time_set_id 
                        AND arnext.generation_type != 'ZEROREADFILLER'
                        AND arnext.id > arcurr.id
                        JOIN MeterRegister mr ON mr.id = arcurr.meter_register_id
                        JOIN Meter mt ON mt.id = mr.meter_id 
                        AND (
                            mt.meter_installation_type LIKE 'COMMS%%' 
                            OR mt.meter_installation_type LIKE 'MR%%'
                            )
                        JOIN AccountUtility au ON au.account_id = arcurr.account_id
                        JOIN Utility ut ON ut.id = au.utility_id
                        JOIN UtilityNetwork un ON un.id = ut.utility_network_id
                        JOIN Jurisdiction ju ON ju.id = un.jurisdiction_id
                        LEFT JOIN UtilityProfile up 
                        ON  up.run_date = %s 
                        AND up.identifier_type = 'Utility' 
                        AND up.identifier = ut.identifier 
                        AND up.interval_date = arcurr.read_date 
                        AND up.nmi_suffix = mr.suffix_id
                        WHERE ut.identifier = %s
                        AND   arcurr.read_date >= %s
                        AND   arcurr.read_date < %s
                        AND   arcurr.plan_type_id = 1
                        AND   arcurr.rev_invoice_id = -1
                        AND   arcurr.plan_item_type_id = 2
                        AND   arcurr.generation_type != 'ZEROREADFILLER'
                        AND   arnext.id IS NULL -- there is not a subsequent matching agg read record
                        AND   up.id IS NULL -- there is no NEM read but there are agg read records
                        GROUP BY 'Utility', ut.identifier, arcurr.read_date, mr.suffix_id, mr.register_id, ju.name
                        HAVING SUM(arcurr.read_value) != 0
                        """, (run_date, source_date, nmi, start_inclusive, end_exclusive)
                    )
                    rows_affected += max(0, cur.rowcount)
                Action.record_processing_summary(cur, run_date, run_type, nmi, start_inclusive, rows_affected)
                self.log(rows_affected, 'variances identified for nmi', nmi)
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise


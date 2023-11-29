DROP TEMPORARY TABLE IF EXISTS tmp_delete_bua_account_summary;

CREATE TEMPORARY TABLE tmp_delete_bua_account_summary(
  identifier INT NOT NULL
  ,INDEX IDX_bua_delete_bua_acc_sum(identifier)
);

INSERT INTO tmp_delete_bua_account_summary(identifier)
SELECT CAST(identifier AS UNSIGNED)
FROM BUAControl
WHERE run_type = 'PrepareExport'
AND status <> 'DONE'
;

DELETE bas
FROM BUAAccountSummary bas
JOIN tmp_delete_bua_account_summary tmp ON tmp.identifier = bas.account_id
;

DROP TEMPORARY TABLE IF EXISTS tmp_delete_bua_account_summary;

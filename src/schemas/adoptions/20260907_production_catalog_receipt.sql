-- Production catalog-adoption receipt.
-- Receipt JSON SHA256: d6ddfa5385401bdc983483f664f73f9fe6a963c99dbce478c9947d8304440209
-- Catalog equivalence observed; no historical migration was executed or recorded.
DO $catalog_adoption$
BEGIN
    IF current_setting('warehouse_control.catalog_adoption_receipt', true)
            IS DISTINCT FROM 'warehouse.production.catalog-adopted.20260907.288b4eaf817b' THEN
        RAISE EXCEPTION 'catalog-adoption root requires a verified adoption transaction';
    END IF;
END
$catalog_adoption$;
SELECT
    'warehouse.production.catalog-adopted.20260907.288b4eaf817b'::text AS catalog_adoption_receipt_id,
    '288b4eaf817bc00f1bba71d4949c72a45d58a77ca0db6907c68a5ae0b337ab94'::text AS observed_catalog_sha256;

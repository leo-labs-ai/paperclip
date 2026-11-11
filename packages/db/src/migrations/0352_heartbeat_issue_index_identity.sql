-- Two published migrations used the same name for different indexes. Preserve
-- the deployed four-key expression index by renaming it, then supply the native
-- issue_id index. Fresh installs need the expression index instead. Never edit
-- the historical SQL: deployed migration history identifies it by hash.
--
-- This migration is deliberately transactional. SHARE blocks writes while the
-- catalogs are checked and any missing index is built; reads remain available.
-- On large heartbeat_runs tables apply during a maintenance window, with enough
-- statement_timeout and disk headroom for one index build. No CONCURRENTLY or
-- nontransactional executor exception is required.
-- paperclip:migration-safety-ignore large-create-index-not-concurrently: this transaction must atomically validate and reconcile both index identities; schedule a maintenance window for the bounded pair of index builds.
DO $$
DECLARE
  target_table oid := 'public.heartbeat_runs'::regclass;
  company_att smallint;
  issue_att smallint;
  created_att smallint;
  id_att smallint;
  index_name text;
  index_oid oid;
  definition record;
  is_native boolean;
  is_expression boolean;
  old_is_expression boolean := false;
  expression_exists boolean := false;
  native_exists boolean := false;
BEGIN
  LOCK TABLE public.heartbeat_runs IN SHARE MODE;
  SELECT attnum INTO STRICT company_att FROM pg_attribute WHERE attrelid = target_table AND attname = 'company_id' AND NOT attisdropped;
  SELECT attnum INTO STRICT issue_att FROM pg_attribute WHERE attrelid = target_table AND attname = 'issue_id' AND NOT attisdropped;
  SELECT attnum INTO STRICT created_att FROM pg_attribute WHERE attrelid = target_table AND attname = 'created_at' AND NOT attisdropped;
  SELECT attnum INTO STRICT id_att FROM pg_attribute WHERE attrelid = target_table AND attname = 'id' AND NOT attisdropped;

  FOREACH index_name IN ARRAY ARRAY['heartbeat_runs_company_issue_created_idx', 'heartbeat_runs_company_ctx_issue_created_id_idx'] LOOP
    index_oid := to_regclass(format('public.%I', index_name));
    IF index_oid IS NULL THEN CONTINUE; END IF;
    SELECT i.*, am.amname,
      pg_get_expr(i.indexprs, i.indrelid) AS expression,
      ARRAY(SELECT n.nspname || '.' || o.opcname FROM unnest(i.indclass::oid[]) WITH ORDINALITY AS k(oid, position)
            JOIN pg_opclass o ON o.oid = k.oid JOIN pg_namespace n ON n.oid = o.opcnamespace ORDER BY k.position) AS opclasses
      INTO definition
      FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am am ON am.oid = c.relam
      WHERE i.indexrelid = index_oid;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'Cannot reconcile %: name belongs to a non-index relation', index_name;
    END IF;
    IF definition.indrelid <> target_table OR definition.amname <> 'btree'
       OR NOT definition.indisvalid OR NOT definition.indisready OR NOT definition.indislive
       OR definition.indisunique OR definition.indisprimary OR definition.indisexclusion
       OR definition.indpred IS NOT NULL OR definition.indnatts <> definition.indnkeyatts THEN
      RAISE EXCEPTION 'Cannot reconcile %: unexpected index properties; inspect pg_get_indexdef before retrying', index_name;
    END IF;
    is_native := definition.indnkeyatts = 3
      AND definition.indkey::text = format('%s %s %s', company_att, issue_att, created_att)
      AND definition.indoption::text = '0 0 0'
      AND definition.indcollation::text = '0 0 0'
      AND definition.indexprs IS NULL
      AND definition.opclasses = ARRAY['pg_catalog.uuid_ops', 'pg_catalog.uuid_ops', 'pg_catalog.timestamptz_ops'];
    is_expression := definition.indnkeyatts = 4
      AND definition.indkey::text = format('%s 0 %s %s', company_att, created_att, id_att)
      AND definition.indoption::text = '0 0 3 3'
      AND definition.indcollation::text = format('0 %s 0 0', 'pg_catalog."default"'::regcollation::oid)
      AND definition.expression = '(context_snapshot ->> ''issueId''::text)'
      AND definition.opclasses = ARRAY['pg_catalog.uuid_ops', 'pg_catalog.text_ops', 'pg_catalog.timestamptz_ops', 'pg_catalog.uuid_ops'];
    IF index_name = 'heartbeat_runs_company_issue_created_idx' THEN
      IF COALESCE(is_native, false) THEN native_exists := true;
      ELSIF COALESCE(is_expression, false) THEN old_is_expression := true;
      ELSE RAISE EXCEPTION 'Cannot reconcile %: unknown definition; inspect pg_get_indexdef before retrying', index_name;
      END IF;
    ELSE
      IF NOT COALESCE(is_expression, false) THEN
        RAISE EXCEPTION 'Cannot reconcile %: unknown definition; inspect pg_get_indexdef before retrying', index_name;
      END IF;
      expression_exists := true;
    END IF;
  END LOOP;

  IF old_is_expression THEN
    IF expression_exists THEN
      -- Both definitions were checked above; retain the already canonical copy.
      DROP INDEX public.heartbeat_runs_company_issue_created_idx;
    ELSE
      ALTER INDEX public.heartbeat_runs_company_issue_created_idx RENAME TO heartbeat_runs_company_ctx_issue_created_id_idx;
      expression_exists := true;
    END IF;
  END IF;
  IF NOT native_exists THEN
    CREATE INDEX heartbeat_runs_company_issue_created_idx ON public.heartbeat_runs USING btree (company_id, issue_id, created_at);
  END IF;
  IF NOT expression_exists THEN
    CREATE INDEX heartbeat_runs_company_ctx_issue_created_id_idx ON public.heartbeat_runs USING btree (company_id, (context_snapshot->>'issueId'), created_at DESC, id DESC);
  END IF;
END $$;

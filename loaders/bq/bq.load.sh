GENETIC_OUT_BUCKET=genetics-portal-data/out

bq --location=EU load --skip_leading_rows=1 -F "tab" --quote '' --source_format=CSV g2v_draft.`date +%Y%m%d` "gs://${GENETIC_OUT_BUCKET}/v2g/*.csv" ./bq.v2g.schema.json

bq --location=EU load --skip_leading_rows=1 -F "tab" --quote '' --source_format=CSV v2d_draft.`date +%Y%m%d` "gs://${GENETIC_OUT_BUCKET}/v2d/*.csv" ./bq.v2d.schema.json

bq --location=EU load -F "tab" --quote '' --source_format=CSV v2g_lut_gene.`date +%Y%m%d` "gs://${GENETIC_OUT_BUCKET}/v2g-lut-gene/*.csv" gene,chr,start:integer,end:integer

bq --location=EU load -F "tab" --quote '' --source_format=CSV v2g_lut_rsid.`date +%Y%m%d` "gs://${GENETIC_OUT_BUCKET}/v2g-lut-rsid/*.csv" rsid,chr,position:integer

bq --location=EU load --skip_leading_rows=1 -F "tab" --quote '' --source_format=CSV d2v2g_draft.`date +%Y%m%d` "gs://${GENETIC_OUT_BUCKET}/d2v2g/*.csv" ./bq.d2v2g.schema.json

{#-- Where download_aemo.py lands the raw CSVs. ONE folder for both engines, landed PLAIN
     because Fabric OPENROWSET cannot read gzip CSV at all — and so both engines read the
     same bytes; without that, a parity comparison means nothing. --#}
{%- macro get_csv_archive_path() -%}
{{ get_root_path() ~ '/csv_raw' }}
{%- endmacro -%}

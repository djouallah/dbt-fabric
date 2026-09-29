{#-- The landing root, for every engine — one name, so a path bug cannot be
     engine-specific. --#}
{%- macro get_root_path() -%}
{{ env_var('FILES_PATH', '/tmp') | trim }}
{%- endmacro -%}

-- ==============================================================================
-- cents_to_dollars.sql — MACRO (Reusable SQL Function)
-- ==============================================================================
--
-- WHAT IS A dbt MACRO?
--   A macro is a reusable piece of SQL written in Jinja (dbt's templating
--   language). Think of it like a Python function — you define it once and
--   call it from any model. dbt compiles the macro into raw SQL at build time.
--
-- WHAT IS JINJA?
--   Jinja is a templating language (originally from Python/Flask). In dbt:
--   - {% ... %} = control flow (if/for/macro definitions)
--   - {{ ... }} = output expressions (variables, function calls)
--   - {# ... #} = comments (not included in compiled SQL)
--
-- HOW TO USE THIS MACRO:
--   In any model SQL file:
--     SELECT {{ cents_to_dollars('price_in_cents') }} AS price_in_dollars
--
--   This compiles to:
--     SELECT (price_in_cents / 100)::number(18, 2) AS price_in_dollars
--
-- PARAMETERS:
--   column_name: The column containing values in cents (required)
--   precision:   Decimal places in the output (optional, default=2)
--
-- LEARNING TIP:
--   Macros prevent copy-paste errors. Without this macro, every developer
--   would write their own cents-to-dollars conversion, potentially with
--   different precision or rounding rules. One macro = one source of truth.
-- ==============================================================================

{% macro cents_to_dollars(column_name, precision=2) %}
    {# Divide by 100 to convert cents to dollars, cast to fixed precision #}
    ({{ column_name }} / 100)::number(18, {{ precision }})
{% endmacro %}

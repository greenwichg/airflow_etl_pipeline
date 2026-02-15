{% macro generate_date_spine(start_date, end_date) %}
    /*
        Generates a continuous sequence of dates between start_date and end_date.
        Useful for gap-filling daily aggregation tables.
    */
    select
        dateadd(day, seq4(), '{{ start_date }}'::date) as date_day
    from table(generator(rowcount => datediff(day, '{{ start_date }}'::date, '{{ end_date }}'::date) + 1))
{% endmacro %}

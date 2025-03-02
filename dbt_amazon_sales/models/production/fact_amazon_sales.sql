{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}


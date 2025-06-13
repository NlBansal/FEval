create user dbt_ro_user with password '';

grant connect on database spotify to dbt_ro_user;
grant usage on schema raw to dbt_ro_user;

grant select on all tables in schema raw to dbt_ro_user

alter default privileges in schema raw grant select  on tables to dbt_ro_user;
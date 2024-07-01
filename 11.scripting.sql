use snow_db;

-- Annon Block of Code since its not named
begin
  create or replace table fruit (
    fruit_id number,
    name varchar,
    color varchar
  );
  create or replace table fruit_inventory (
    fruit_id number,
    inv_date date,
    amount_kg number
  );
end;

-- ------------------------------------
-- Block of Code named as a procedure
-- to execute code use: call create_sample_tables()
create or replace procedure create_sample_tables()
    returns number
    language sql
as
begin
  create or replace table fruit (
    fruit_id number,
    name varchar,
    color varchar
  );
  create or replace table fruit_inventory (
    fruit_id number,
    inv_date date,
    amount_kg number
  );
end;

call create_sample_tables();

-- ------------------------------------
-- Variable Demo
create or replace procedure var_demo()
    returns string
    language sql
as
declare
    fav_fruit string;
    fav_number default (3+3);
    has_fav_artist boolean default false;
begin
    fav_fruit := 'Mango';
    fav_number := fav_number + 1;
    has_fav_artist := not has_fav_artist;

    -- Can't declare same variable twice
    -- let fav_fruit := 'Apple';

    let new_fav_fruit := 'Kiwi';
    
    return fav_fruit || ' | ' || new_fav_fruit || ' | ' || fav_number || ' | ' || has_fav_artist ;
end;

call var_demo();

-- ------------------------------------
-- use variables in sql statements and table identifiers
create or replace procedure add_fruits()
    returns number
    language sql
as
declare
    fruit_id number default 101;
    fruit_name string default 'Orange';
    fruit_color string default 'Orange';
    table_name string default 'fruit';
begin
    insert into identifier(:table_name) values(:fruit_id, :fruit_name, :fruit_color);
end;

call add_fruits();
select * from fruit;

-- ------------------------------------

begin
    let weight_lb := 114;
    if (weight_lb < 100) then
        return 'less than 100';
    elseif (weight_lb = 100) then
        return 'exactly 100';
    else
        return 'more than 100';
    end if;
end;

-- ------------------------------------

begin
    let fruit_name := 'orange';
    case (fruit_name)
        when 'apple' then
            return 'red';
        when 'banana' then
            return 'yellow';
        when 'avocado' then
            return 'green';
        else
            return 'unknown';
    end;
end;

-- ------------------------------------

declare
    counter integer default 0;
    maximum_count integer default 5;
begin
    for i in 1 to maximum_count do
        counter := counter + 1;
    end for;
    return counter;
end;

-- ------------------------------------

declare
    counter integer default 0;
    maximum_count integer default 5;
begin
    while (counter < maximum_count) do
        counter := counter + 1;
    end while;
    return counter;
end;

-- ------------------------------------

declare
    counter integer default 0;
    maximum_count integer default 5;
begin
    loop 
        if (counter < maximum_count) then
            counter := counter + 1;
        else
            break;
        end if;
    end loop;
    return counter;
end;

-- ------------------------------------

insert into fruit values (1, 'apple', 'red'), (2, 'banana', 'yellow'), (3, 'avocado', 'green');
insert into FRUIT_INVENTORY values (1, sysdate(), 110), (2, sysdate(), 80), (3, sysdate(), 145), (101, sysdate(), 257);

-- ------------------------------------

declare
  v_id integer;
  v_name varchar;
  v_color varchar;
begin
    select fruit_id, name, color
    into v_id, v_name, v_color
    from fruit
    where fruit_id = 1;

    return v_id || ', ' || v_name || ', ' || v_color;
end;

-- ------------------------------------

declare
    c1 cursor for
        select amount_kg from fruit_inventory;
    total_amount number default 0;
    begin
        for record in c1 do
            total_amount := total_amount + record.amount_kg;
        end for;
    return total_amount;
end;

-- ------------------------------------

select * from information_schema.procedures;
select * from information_schema.packages;
select * from information_schema.packages where language='scala' order by version desc limit 7;
select * from information_schema.packages where language='java' order by version desc limit 7;
select distinct language, runtime_version from information_schema.packages 
where 1 = 1
and language='python'
and runtime_version is not null;

-- ------------------------------------
-- No begin/end since this is a python function
-- The function is called in select statement
create or replace function convert_temperature(temp_in_cel number)
    returns number
    language python
    runtime_version = '3.9'
    handler = 'f_cel2fah'
as
$$
def f_cel2fah(temp_in_cel):
    return (temp_in_cel * 1.8) + 32
$$;

select convert_temperature(25);

-- ------------------------------------
-- When creating procedure, mention the package name 
create or replace procedure convert_weight(weight_lb number)
  returns number
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'f_convert_weight'
as
$$
def f_convert_weight(weight_lb):
  return weight_lb * 0.453592
$$;

call convert_weight(25);

drop procedure convert_weight(number);
drop function convert_temperature(number);

-- ------------------------------------

-- sqlid is a global variable 
-- returns the query id of the last query executed
-- works inside block of code
begin
    update fruit
    set fruit_id = 4
    where fruit_id = 101;
    return sqlid;
end;

-- Returns the last statement executed, 
-- Returns the query id of the anon block if block of code is executed.
select last_query_id();

begin
    update fruit
    set fruit_id = 204
    where fruit_id = 4;
    return sqlrowcount || ', ' || sqlfound || ', ' || sqlnotfound || ', ' || sqlid;
end;

-- ---------------------
-- Handling Exceptions
-- ---------------------
declare
  v_name varchar;
begin
  select name
  into v_name
  from FRUIT;
  return v_name;
exception
  when statement_error then
    return object_construct('Error type', 'Statement error',
      'SQLCODE', sqlcode, 'SQLERRM', sqlerrm,
      'SQLSTATE', sqlstate);
  when expression_error then
    return object_construct('Error type', 'Expression error',
      'SQLCODE', sqlcode, 'SQLERRM', sqlerrm,
      'SQLSTATE', sqlstate);
  when other then
    return object_construct('Error type', 'Other error',
      'SQLCODE', sqlcode, 'SQLERRM', sqlerrm,
      'SQLSTATE', sqlstate);
end;


-- -20,999 and -20,000
-- Run it with num2 as 0 first
declare
    num1 := 20;
    num2 := 10;
    custom_exception exception(-20800, 'Custom Exception');
begin
    let n := (num1 / num2);
    raise custom_exception;
exception
    when expression_error then 
        return object_construct('Error Type', 
                                'custom_exception', 
                                'SQLCODE', sqlcode,
                                'SQLERRM', sqlerrm,
                                'SQLSTATE', sqlstate
                                );
    when other then
        return object_construct('Error Type', 
                                'custom_exception', 
                                'SQLCODE', sqlcode,
                                'SQLERRM', sqlerrm,
                                'SQLSTATE', sqlstate
                                );    
end;

-- -----------------------
-- Cursors
-- -----------------------
declare
    total_available number default 0;
    fruit_names cursor for select * from fruit_inventory;
begin
    open fruit_names;
    for record in fruit_names do
        total_available := record.amount_kg + total_available;
    end for;
    close fruit_names;
    return total_available;
end;

declare
    fruit_names cursor for select * from fruit;
begin
    open fruit_names;
    return table(resultset_from_cursor(fruit_names));
end;
select 'red-circle', i, i+1 from generate_series(1, 5) i
union all 
select 'blue-triangle', 5-i, i+1 from generate_series(1, 5) i
;

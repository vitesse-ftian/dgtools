drop table linear_train;
drop table linear_eval;
create table linear_train (tag int, x float, y float) distributed randomly;
create table linear_eval (tag int, x float, y float) distributed randomly;
\copy linear_train from './linear_data_train.csv' with csv;
\copy linear_eval from './linear_data_eval.csv' with csv;

drop table moon_train;
drop table moon_eval;
create table moon_train (tag int, x float, y float) distributed randomly;
create table moon_eval (tag int, x float, y float) distributed randomly;
\copy moon_train from './moon_data_train.csv' with csv;
\copy moon_eval from './moon_data_eval.csv' with csv;

drop table saturn_train;
drop table saturn_eval;
create table saturn_train (tag int, x float, y float) distributed randomly;
create table saturn_eval (tag int, x float, y float) distributed randomly;
\copy saturn_train from './saturn_data_train.csv' with csv;
\copy saturn_eval from './saturn_data_eval.csv' with csv;

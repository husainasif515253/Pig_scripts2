employee = load '/user/ubuntu1/husain/practice_datasets/employee.txt' using PigStorage(',') as (id:long, name:chararray, phone:long, device_id:chararray, timings:chararray);
employee = foreach employee generate ToDate(timings,'YYYY-MM-dd HH:mm:ss.S') as timings,id as id,name as name,phone as phone,device_id as device_id, SUBSTRING(timings,0,10) as date;
employee = order employee by timings;


device = load '/user/ubuntu1/husain/practice_datasets/device.txt' using PigStorage(',') as (country:chararray, city:chararray, organization:chararray, device:chararray, device_id:chararray, status:chararray);



emp_dev = join employee by device_id, device by device_id;
emp_dev = order emp_dev by timings;
emp_dev = foreach emp_dev generate employee::timings as timings,employee::id as id,employee::name as name,employee::phone as phone,employee::device_id as device_id,employee::date as date,device::country as country,device::city as city,device::organization as organization,device::device as device,device::status as status;


in = filter emp_dev by status matches 'in';
in = order in by timings;
in = rank in by timings;
in = foreach in generate rank_in as rnk,timings as in_timings,id as id, name as name,phone as phone,device_id as device_id,date as date,country as country,city as city,organization as organization,device as device, status as status;


out = filter emp_dev by status matches 'out';
out = order out by timings;
out = rank out by timings;
out = foreach out generate rank_out as rnk, timings as out_timings;

in_out = join in by rnk, out by rnk;
in_out = foreach in_out generate in::rnk as rank,in::id as id,in::name as name,in::date as date,in::country as country,in::city as city,in::phone as phone,in::device_id as device_id,in::organization as organization,in::device as device,out::out_timings as outtime,in::in_timings as intime;
in_out = foreach in_out generate rank as rank,id as id,name as name,date as date,country as country,city as city,phone as phone,device_id as device_id,organization as organization,device as device,outtime as outtime,intime as intime, MinutesBetween(outtime,intime) as productive_hours;



total_hours = group in_out by (id,date,name);
x = foreach total_hours generate group.id as id, group.name as name, group.date as date, MAX(in_out.outtime) as outtime, MIN(in_out.intime) as intime,MinutesBetween(MAX(in_out.outtime),MIN(in_out.intime)) as total_hours , SUM(in_out.productive_hours) as productive_hours;
report = foreach x generate name as name, id as id, date as date, (float)total_hours/60 as total_hours, (float)productive_hours/60 as productive_hours, (float)(total_hours-productive_hours)/60 as hours_out;

set mapreduce.map.memory.mb 2048;set mapreduce.map.java.opts -Xmx2048m;set mapreduce.reduce.memory.mb 2048;set mapreduce.reduce.java.opts -Xmx2048m;
fs -rm -R /user/ubuntu1/husain/final_oozie_pig/pig_object
STORE report INTO '/user/ubuntu1/husain/final_oozie_pig/pig_object' using PigStorage(',','-schema');

set mapreduce.map.memory.mb 2048;set mapreduce.map.java.opts -Xmx2048m;set mapreduce.reduce.memory.mb 2048;set mapreduce.reduce.java.opts -Xmx2048m;
fs -rm /user/ubuntu1/husain/final_oozie_pig/pig_object/.pig_schema;

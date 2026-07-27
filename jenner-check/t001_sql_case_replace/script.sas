/* SAS SQL solution from the repo, adapted to run standalone.               */
/* Only change vs. the original: the LIBNAME sd1 "d:/sd1" reference is       */
/* dropped and SD1.HAVE is created in WORK, so the exact PROC SQL CASE logic */
/* runs anywhere without a local disk path. The DATA step, column list, and  */
/* the CASE WHEN signal <> 0 THEN signal ELSE trade END are unchanged.       */

options validvarname=upcase;

data have;
input symbol $ signal trade;
cards4;
s1 0 .
s1 1 1
s2 2 .
s2 0 -1.0
;;;;
run;quit;

proc sql;
  create
      table want as
  select
      symbol
     ,signal
     ,case
        when signal <> 0 then signal
        else trade
      end as trade
  from
     have
;quit;

proc print data=want;
run;quit;

%let pgm=utl-if-col2-is-not-equal-to-zero-then-replace-coll3-with-col2-in-sql-sas-r-python;

If col2 is not equal to zero then replace coll3 with col2 in sql sas r python

Too long to post in SAS-L

%stop_submission;

         Three Solutions
             1 sas sql
             2 r sql
             3 python sql
             4 repos that involve R and Python and SAS(WPS)

This is very simple coding, however R and Python programmers seem to appriciate this?

github
https://tinyurl.com/4eutw5j6
https://github.com/rogerjdeangelis/if-col2-is-not-equal-to-zero-then-replace-coll3-with-col2-in-sql-sas-r-python

stackoverflow
https://tinyurl.com/3ujaehf4
https://stackoverflow.com/questions/78914882/replace-a-cell-in-a-column-based-on-a-cell-in-another-column-in-a-polars-datafra

/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                             |                                                     |                                    */
/*                             |                                                     |                                    */
/*              INPUT          |             PROCESS                                 | OUTPUT                             */
/*                             |     Same in SAS, R and Python                       |                                    */
/*  SD1.HAVE                   |                                                     | WORK.WANT                          */
/*                             |                                                     |                                    */
/*  SYMBOL    SIGNAL    TRADE  | proc sql;                                           | SYMBOL SIGNAL TRADE                */
/*                             |  create                                             |                                    */
/*    s1         0         .   |      table want as                                  |   s1      0      .                 */
/*    s1         1         1   |  select                                             |   s1      1      1 (signal value)  */
/*    s2         2         .   |      symbol                                         |   s2      2      2 (signal value)  */
/*    s2         0        -1   |     ,signal                                         |   s2      0     -1                 */
/*                             |     ,case                                           |                                    */
/*                             |        when signal <> 0 then signal                 |                                    */
/*                             |        else trade                                   |                                    */
/*                             |      end as trade                                   |                                    */
/*                             |  from                                               |                                    */
/*                             |      sd1.have                                       |                                    */
/*                             | ;quit;                                              |                                    */
/*                             |                                                     |                                    */
/*-----------------------------+---------------------------------------------------+--------------------------------------*/
/*                             |                                                     |                                    */
/*                             |  PYTHON                                             |                                    */
/*                             |                                                     |                                    */
/*                             |   import polars as pl                               |                                    */
/*                             |   df.with_columns(                                  |                                    */
/*                             |     trade=pl.when(                                  |                                    */
/*                             |       pl.col("signal").first().over("symbol") != 0, |                                    */
/*                             |       pl.int_range(pl.len()).over("symbol") == 0,   |                                    */
/*                             |     ).then("signal").otherwise("trade")             |                                    */
/*                             |   )                                                 |                                    */
/*                             |                                                     |                                    */
/**************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
input symbol $ signal trade;
cards4;
s1 0 .
s1 1 1
s2 2 .
s2 0 -1.0
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  SD1.HAVE total obs=4                                                                                                  */
/*                                                                                                                        */
/*  Obs    SYMBOL    SIGNAL    TRADE                                                                                      */
/*                                                                                                                        */
/*   1       s1         0         .                                                                                       */
/*   2       s1         1         1                                                                                       */
/*   3       s2         2         .                                                                                       */
/*   4       s2         0        -1                                                                                       */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                             _
/ |  ___  __ _ ___   ___  __ _| |
| | / __|/ _` / __| / __|/ _` | |
| | \__ \ (_| \__ \ \__ \ (_| | |
|_| |___/\__,_|___/ |___/\__, |_|
                            |_|
*/

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
     sd1.have
;quit;

*/ /**************************************************************************************************************************/
*/ /*
*/ /*  SD1.WANT total obs=4
*/ /*
*/ /*  Obs    SYMBOL    SIGNAL    TRADE
*/ /*
*/ /*   1       s1         0         .
*/ /*   2       s1         1         1
*/ /*   3       s2         2         2
*/ /*   4       s2         0        -1
*/ /*
*/ /**************************************************************************************************************************/

/*___                     _
|___ \   _ __   ___  __ _| |
  __) | | `__| / __|/ _` | |
 / __/  | |    \__ \ (_| | |
|_____| |_|    |___/\__, |_|
                       |_|
*/

%utl_rbeginx;
parmcards4;
library(sqldf)
library(haven)
source("c:/oto/fn_tosas9x.R");
have<-read_sas("d:/sd1/have.sas7bdat")
want<-sqldf('
   select
       symbol
      ,signal
      ,case
         when signal <> 0 then signal
         else trade
       end as trade
   from
      have
  ')
want
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="rwant"
     );
;;;;
%utl_rendx;

libname sd1 "d:/sd1";

proc print data=sd1.rwant;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  R                                                                                                                     */
/*                                                                                                                        */
/*   > want                                                                                                               */
/*     SYMBOL SIGNAL trade                                                                                                */
/*   1     s1      0    NA                                                                                                */
/*   2     s1      1     1                                                                                                */
/*   3     s2      2     2                                                                                                */
/*   4     s2      0    -1                                                                                                */
/*                                                                                                                        */
/*  SAS                                                                                                                   */
/*                                                                                                                        */
/*  ROWNAMES    SYMBOL    SIGNAL    TRADE                                                                                 */
/*                                                                                                                        */
/*      1         s1         0         .                                                                                  */
/*      2         s1         1         1                                                                                  */
/*      3         s2         2         2                                                                                  */
/*      4         s2         0        -1                                                                                  */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____             _               _   _
|___ /   ___  __ _| |  _ __  _   _| |_| |__   ___  _ __
  |_ \  / __|/ _` | | | `_ \| | | | __| `_ \ / _ \| `_ \
 ___) | \__ \ (_| | | | |_) | |_| | |_| | | | (_) | | | |
|____/  |___/\__, |_| | .__/ \__, |\__|_| |_|\___/|_| |_|
                |_|   |_|    |___/
*/

%utl_pybeginx;
parmcards4;
import pyperclip
import os
from os import path
import sys
import subprocess
import time
import pandas as pd
import pyreadstat as ps
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have, meta = ps.read_sas7bdat("d:/sd1/have.sas7bdat")
exec(open('c:/temp/fn_tosas9.py').read())
want = pdsql("""
  select
      symbol
     ,signal
     ,case
        when signal <> 0 then signal
        else trade
      end as trade
  from
     have
""")
print(want)
fn_tosas9(
   want
   ,dfstr="want"
   ,timeest=3
  )
;;;;
%utl_pyendx;

libname tmp "c:/temp";
proc print data=tmp.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* PYTHON                                                                                                                 */
/*                                                                                                                        */
/*    SYMBOL  SIGNAL  trade                                                                                               */
/*  0     s1     0.0    NaN                                                                                               */
/*  1     s1     1.0    1.0                                                                                               */
/*  2     s2     2.0    2.0                                                                                               */
/*  3     s2     0.0   -1.0                                                                                               */
/*                                                                                                                        */
/* SAS                                                                                                                    */
/*                                                                                                                        */
/* Obs    SYMBOL    SIGNAL    TRADE                                                                                       */
/*                                                                                                                        */
/*  1       s1         0         .                                                                                        */
/*  2       s1         1         1                                                                                        */
/*  3       s2         2         2                                                                                        */
/*  4       s2         0        -1                                                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

options ls=255 ps=80;
libname git "d:/git";

data sub;
 length upc $255;
 set git.git_010_repos;
   upc=upcase(repo);
   if  (index(upc,'-SAS') or index(upc,'_SAS') or index(upc,'_WPS') or index(upc,'-WPS'))
   and  index(upc,'PYTHON')
   and  (index(upc,'-R-') or index(upc,'_R_'));
   DROP UPC;
run;quit;

repos

All obs from sub total obs=102 26AUG2024:10:36:56

Obs    REPO

  1    https://github.com/rogerjdeangelis/utl-add-a-column-to-an-existing-wps-r-python-dataframe-for-loop
  2    https://github.com/rogerjdeangelis/utl-add-zero-counts-for-missing-category-combinations-sas-sparse-completetypes-sql-r-python
  3    https://github.com/rogerjdeangelis/utl-all-combination-of-of-rows-of-dataset-in-wps-r-best-in-wps-r-python
  4    https://github.com/rogerjdeangelis/utl-analyzing-mean-and-median-by-groups-in-sas-wps-r-python
  5    https://github.com/rogerjdeangelis/utl-append-rows-and-merge-with-reference-table-in-sql-wps-r-and-python
  6    https://github.com/rogerjdeangelis/utl-bigint-longint-in-sas-r-and-python
  7    https://github.com/rogerjdeangelis/utl-calculate-percentage-by-group-in-wps-r-python-excel-sql-no-sql
  8    https://github.com/rogerjdeangelis/utl-calculate-regression-coeficients-in-base-sas-fcmp-proc-reg-r-and-python
  9    https://github.com/rogerjdeangelis/utl-calculating-median-values-by-group-wps-r-python-sql
 10    https://github.com/rogerjdeangelis/utl-calculations-involving-a-sql-self-join-in-wps-r-and-python
 11    https://github.com/rogerjdeangelis/utl-cartesian-join-with-condition-in-sql-wps-r-python
 12    https://github.com/rogerjdeangelis/utl-change-from-baseline-to-week1-to-week8-using-wps-r-python-base-and-sql
 13    https://github.com/rogerjdeangelis/utl-change-from-baseline-using-sql-in-sas-r-and-python
 14    https://github.com/rogerjdeangelis/utl-chiSquare-anaysis-in-sas-r-python-using-matrix-algebra-procs-and-sql
 15    https://github.com/rogerjdeangelis/utl-classic-transpose-by-index-variableid-and-value-in-sas-r-and-python
 16    https://github.com/rogerjdeangelis/utl-collasping-categories-into-three-super-categories-in-wps-r-and-python-multi-language
 17    https://github.com/rogerjdeangelis/utl-common-products-sold-by-two-stores-split-string-pivot-long-sql-wps-r-and-python
 18    https://github.com/rogerjdeangelis/utl-compute-the-mean-by-group-using-sas-wps-python-r-native-code-and-sql
 19    https://github.com/rogerjdeangelis/utl-converting-common-wps-coding-to-r-and-python
 20    https://github.com/rogerjdeangelis/utl-converting-multiple-columns-from-numeric-to-character-in-sql-wps-r-python
 21    https://github.com/rogerjdeangelis/utl-converting-sas-proc-rank-to-wps-python-r-sql
 22    https://github.com/rogerjdeangelis/utl-converting-sas-proc-sql-code-to-r-and-python
 23    https://github.com/rogerjdeangelis/utl-count-the-occurance-of-twenty-states-in-a--matrix-of-states-and-provinces-wps-r-python
 24    https://github.com/rogerjdeangelis/utl-create-a-simple-n-percent-clinical-table-in-r-sas-wps-python-output-pdf-rtf-xlsx-html-list
 25    https://github.com/rogerjdeangelis/utl-create-equally-spaced-values-using-partitioning-in-sql-wps-r-python
 26    https://github.com/rogerjdeangelis/utl-create-new-column-based-on-complex-logic-involving-four-other-columns-in-wps-r-python-sql-nosql
 27    https://github.com/rogerjdeangelis/utl-create-new-column-by-concatenating-existing-columns-in-wps-r-and-python-tables
 28    https://github.com/rogerjdeangelis/utl-create-summary-statistics-datasets-in-sql-wps-r-python
 29    https://github.com/rogerjdeangelis/utl-create-tables-from-xml-files-using-sas-wps-r-and-python
 30    https://github.com/rogerjdeangelis/utl-creating-spss-tables-from-a-sas-datasets-using-sas-r-and-python
 31    https://github.com/rogerjdeangelis/utl-dealing-with-missing-values-consitently-within-and-between-multiple-languages-sas-R-and-python
 32    https://github.com/rogerjdeangelis/utl-determinating-gender-from-firstname-AI-sas-r-and-python
 33    https://github.com/rogerjdeangelis/utl-distance-between-a-point-and-curve-in-sql-and-wps-pythony-r-sympy
 34    https://github.com/rogerjdeangelis/utl-drop-down-using-dosubl-from-sas-datastep-to-wps-r-perl-powershell-python-msr-vb
 35    https://github.com/rogerjdeangelis/utl-dropdown-from-SAS-and-run-proc-sql-like-code-in-R-and-Python
 36    https://github.com/rogerjdeangelis/utl-efficiently-swap-values-between-related-columns-in-wps-r-python-sql-nosql
 37    https://github.com/rogerjdeangelis/utl-examples-of-drop-downs-from-sas-to-wps-r-microsoftR-python-perl-powershell
 38    https://github.com/rogerjdeangelis/utl-exporting-python-panda-dataframes-to-wps-r-using-a-shared-sqllite-database
 39    https://github.com/rogerjdeangelis/utl-frequency-of-duplicated-digits-in-social-security-numbers-in-wps-r-python-sql
 40    https://github.com/rogerjdeangelis/utl-given-daily-death-totals-summarize-by-month-wps-r-and-python
 41    https://github.com/rogerjdeangelis/utl-group-by-id-an-subtract-fist.date-from-last.date-using-wps-r-and-python-native-and-sql
 42    https://github.com/rogerjdeangelis/utl-how-to-compare-one-set-of-columns-with-another-set-of-columns-wps-r-python-sql
 43    https://github.com/rogerjdeangelis/utl-how-to-sum-a-variable-by-group-in-sas-r-and-python-using-sql
 44    https://github.com/rogerjdeangelis/utl-importing-sas-tables-sas7bdats-and-sas7bcats-into-python-and-r-with-associared-format-catalogs
 45    https://github.com/rogerjdeangelis/utl-insert-four-rows-after-each-row-so-each-new-row-is-one-hour-later-wps-r-python-datetimes
 46    https://github.com/rogerjdeangelis/utl-last-value-carried-backwards-using-mutate-dow-sql-in-wps-sas-r-python
 47    https://github.com/rogerjdeangelis/utl-left-join-on-id-and-date-between-start-and-end-in-wps-r-and-python
 48    https://github.com/rogerjdeangelis/utl-left-join-two-datasets-to-a-master-dataset-native-and-sql-using-wps-sas-r-and-python
 49    https://github.com/rogerjdeangelis/utl-leveraging-your-knowledge-of-perl-regex-to-sas-wps-r-python-and-perl
 50    https://github.com/rogerjdeangelis/utl-leveraging-your-knowledge-of-regular-expressions-to-wps-r-python-multi-language
 51    https://github.com/rogerjdeangelis/utl-linear-regression-in-python-R-and-sas
 52    https://github.com/rogerjdeangelis/utl-merging-inner-join-dataframes-based-on-single-primary-key-in-wps-r-python-sql-nosql
 53    https://github.com/rogerjdeangelis/utl-merging-two-tables-without-any-common-column-data-in-r-python-and-sas
 54    https://github.com/rogerjdeangelis/utl-minimmum-code-to-transpose-and-summarize-a-skinny-to-fat-with-sas-wps-r-and-python
 55    https://github.com/rogerjdeangelis/utl-monty-hall-problem-r-sas-python
 56    https://github.com/rogerjdeangelis/utl-mysql-queries-without-sas-using-r-python-and-wps
 57    https://github.com/rogerjdeangelis/utl-native-r-and-r-python-sas-sql-calculate-the-standard-deviation-of-all-columns
 58    https://github.com/rogerjdeangelis/utl-nearest-sales-date-on-or-before-a-commercial-date-using-r-roll-join-and-wps-r-and-python-sql
 59    https://github.com/rogerjdeangelis/utl-overall-frequency-of-values-over-all-columns-and-rows-simutaneously-sas-r-python
 60     https://github.com/rogerjdeangelis/utl-partial-key-matching-and-luminosity-in-gene-analysis-sas-r-python-postgresql
 61    https://github.com/rogerjdeangelis/utl-partial-sql-join-based-on-a-column-value-in-wps-sas-r-and-python
 62    https://github.com/rogerjdeangelis/utl-passing-r-python-and-sas-macro-vars-to-sqllite-interface-arguments
 63    https://github.com/rogerjdeangelis/utl-pivot-long-pivot-wide-transpose-partitioning-sql-arrays-wps-r-python
 64     https://github.com/rogerjdeangelis/utl-pivot-long-transpose-three-arrays-of-size-three-sas-r-python-sql
 65    https://github.com/rogerjdeangelis/utl-pivot-transpose-by-id-using-wps-r-python-sql-using-partitioning
 66    https://github.com/rogerjdeangelis/utl-proc-summary-in-sas-R-and-python-sql
 67    https://github.com/rogerjdeangelis/utl-python-import-sas-catalogs-and-export-to-R-dataframe
 68    https://github.com/rogerjdeangelis/utl-Python-import-standalone-sas-format-catalogs-and-export-to-R-dataframes
 69    https://github.com/rogerjdeangelis/utl-python-panda-dataframes-to-R-dataframes-and-SAS-V5-xport-files
 70    https://github.com/rogerjdeangelis/utl-python-r-and-sas-sql-solutions-to-add-missing-rows-to-a-data-table
 71    https://github.com/rogerjdeangelis/utl-python-r-import-a-subset-of-SAS-columns-from-sas7bdat-and-v5-export-files
 72    https://github.com/rogerjdeangelis/utl-r-python-sas-sqlite-subtracting-the-means-of-a-specific-column-from-other-columns
 73    https://github.com/rogerjdeangelis/utl-read-print-file-backwards-in-perl-powershell-sas-r-and-python
 74    https://github.com/rogerjdeangelis/utl-rename-and-cast-char-to-numeric-using-do-over-arrays-in-natve-and-mysql-wps-r-python
 75    https://github.com/rogerjdeangelis/utl-replace-missing-values-with-column-means-wps-r-python
 76    https://github.com/rogerjdeangelis/utl-sas-fcmp-hash-stored-programs-python-r-functions-to-find-common-words
 77    https://github.com/rogerjdeangelis/utl-sas-proc-transpose-in-sas-r-wps-python-native-and-sql-code
 78    https://github.com/rogerjdeangelis/utl-sas-proc-transpose-wide-to-long-in-sas-wps-r-python-native-and-sql
 79    https://github.com/rogerjdeangelis/utl-select-all-unique-pairs-of-ingredients-in-salads-r-wps-r-python-sql
 80    https://github.com/rogerjdeangelis/utl-select-distinct-values-of-one-variables-grouped-by-another-variable-in-wps-r-and-python
 81    https://github.com/rogerjdeangelis/utl-select-groups-of-rows-having-a-compound-condition-and-further-subset-using-wps-r-python-sql
 82    https://github.com/rogerjdeangelis/utl-select-students-that-have-not-transfered-schools-over-the-last-three-years-wps-r-python-sql
 83    https://github.com/rogerjdeangelis/utl-select-the-diagonal-values-from-a-dataset-in-excel-r-wps-python
 84    https://github.com/rogerjdeangelis/utl-select-the-first-two-observations-from-each-group-wps-r-python-sql
 85    https://github.com/rogerjdeangelis/utl-self-join-get-all-combinations-where-left-col-less-then-right-col-sql-wps-r-and-python
 86    https://github.com/rogerjdeangelis/utl-set-type-for-subject-based-on-baseline-dose-wps-r-python-sql
 87    https://github.com/rogerjdeangelis/utl-simple-classic-transpose-pivot-wider-in-native-and-sql-wps-r-python
 88    https://github.com/rogerjdeangelis/utl-simple-conditional-summarization-in-sql-wps-r-and-python-multi-language
 89    https://github.com/rogerjdeangelis/utl-simple-left-join-in-native-r-and-sql-wps-r-and-python
 90    https://github.com/rogerjdeangelis/utl-sort-each-column-independently-from-lowest-to-highest-wps-r-python
 91    https://github.com/rogerjdeangelis/utl-summarizing-data-in-SAS-WPS-Python-R-using-native-code-and-sql
 92    https://github.com/rogerjdeangelis/utl-top-four-seasonal-precipitation-totals--european-cities-sql-partitions-in-wps-r-python
 93    https://github.com/rogerjdeangelis/utl-transpose-fat-to-skinny-pivot-longer-in-sas-wps-r-pythonv
 94    https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
 95    https://github.com/rogerjdeangelis/utl-tumbling-goups-of-ten-temperatures-similar-like-rolling-and-moving-means-wps-r-python
 96    https://github.com/rogerjdeangelis/utl-update-an-existing-excel-named-range-R-python-sas
 97    https://github.com/rogerjdeangelis/utl-update-master-using-transaction-data-using-sql-in-sas-r-and-python
 98    https://github.com/rogerjdeangelis/utl-using-column-position-instead-of-excel-column-names-due-to-misspellings-sas-r-python
 99    https://github.com/rogerjdeangelis/utl-using-nonlinear-optimization-to-fit-an-arc-tangent-curve-wps-r-python
100    https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning
101    https://github.com/rogerjdeangelis/utl-very-simple-sql-join-and-summary-in-python-r-wps-and-sas
102    https://github.com/rogerjdeangelis/utl_SAS_dataset_to_json_using_SAS_R_Python_and_WPS

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/

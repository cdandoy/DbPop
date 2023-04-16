#!/bin/sh
##
## This script runs every time DbPop is started.
## In this example, we generate many rows for performance testing.
##

echo Generating employees
{
  echo "employee_id,employee_name,department_id,hiredate"
  for i in $(seq 6 10005); do echo "$i,Employee_$i,3,{{today}}"; done
} >/var/opt/dbpop/datasets/performance/demo/hr/employees.csv

echo Generating departments
{
  echo "department_id,department_name,office_id"
  for i in $(seq 5 10004); do echo "$i,Department_$i,1"; done
} >/var/opt/dbpop/datasets/performance/demo/hr/departments.csv

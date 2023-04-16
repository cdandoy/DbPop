CREATE TABLE demo.hr.offices
(
    office_id   INT IDENTITY PRIMARY KEY,
    office_name VARCHAR(256) NOT NULL
)
GO
CREATE TABLE demo.hr.departments
(
    department_id   INT IDENTITY PRIMARY KEY,
    department_name VARCHAR(256) NOT NULL,
    office_id       INT          NOT NULL REFERENCES demo.hr.offices
)
GO
CREATE TABLE demo.hr.employees
(
    employee_id   INT IDENTITY PRIMARY KEY,
    employee_name VARCHAR(256) NOT NULL,
    department_id INT          NOT NULL REFERENCES demo.hr.departments,
    hiredate      DATE
)
GO

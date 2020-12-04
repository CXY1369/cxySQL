@Author: Xingyu Chen
//Project: Database Management System engine design
//Date    : 2020-08
=====================================
//Developing Platform: Eclipse 2019
// java version "1.8.0_212"
//Java(TM) SE Runtime Environment (build 1.8.0_212-b10)

======================================

1. Read User Guide in Detail and follow the instructions
2. Run the CXYsql program in command line or on other supporting platforms like Eclipse.
    Please refer to the User Guide if you have problem running the program.
3. Input query languages similar to mySql
4. Supported Data Types: Byte, Short, Integer, Long, Float, Double, Year, Time, Datetime, Date, Text
5. The two directories "data\catalog" and "data\user_data" will be generated when you open the data base for the first time.
6. User defined data table files are under "data\user_data"
7. System catalog files are under "data\catalog".

Notice: Do Not use Tinybyte, Small Int, Int , BigInt, Real
            Currently doesn't support these terms

            There must be space in the conditional WHERE clause !!!

=======================================
Current supporting Operations & Syntax (Insensitive to letter case)

1. To see all the table files existing in the database

    Show Tables;

2. To create a new table file

    Create Table  User_defined_table_name ( Column_name_1, Data type, 
				     Column_name_2, Data type ........);

    Example:
    Create table Student (Name Text, Age Integer, Sex Text);

3. Insert data into a table file

   Insert into Table_name (Column_1, Column_2,.....)
                             Values (Value_1,    Value_2,.........);

   Example:
   Insert Into Student (Name Age Sex)
                           Values  (Sue  12  F  );
   Insert Into Student (Name Age Sex)
                           Values  (Sandy 10 M );

4. Query / Search for the data from a table file

    Select * From Table_name; // return all rows with every column of this table

    Select column_1, column_2,... From Table_name; // return all rows with specified column of this table

    Select column_1, column_2,... From Table_name Where condition_1 and/or condition_2 and/or condition_3.....; // return specified columns in the rows that meet the conditions

    Example:
    Select Name, Age From Student Where Age > 10 and Sex = M;

5. Delete data from a table file

    Delete From Table_name Where condition_1 and/or condition_2 and/or condition_3 and/or....;

    Example:
    Delete From Student Where Age > 10 or Sex = M;

6. Remove a table file

    Drop Table table_name;

    Example:
    Drop Table Student;
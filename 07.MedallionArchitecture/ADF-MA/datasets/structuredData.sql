-- Create table
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Department VARCHAR(50),
    Salary DECIMAL(10,2)
);

-- Insert data
INSERT INTO Employees (EmployeeID, FirstName, LastName, Department, Salary)
VALUES
    (1, 'John', 'Smith', 'IT', 75000.00),
    (2, 'Sarah', 'Johnson', 'HR', 65000.00),
    (3, 'Michael', 'Brown', 'Finance', 80000.00);

-- Another table
CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(50)
);

-- Insert data
INSERT INTO Departments (DepartmentID, DepartmentName)
VALUES
    (1, 'IT'),
    (2, 'HR'),
    (3, 'Finance');
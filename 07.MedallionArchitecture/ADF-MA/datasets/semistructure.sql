CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    ContactInfo JSON
);

INSERT INTO Customers
VALUES
(
    1,
    'John Smith',
    '{
        "email":"john@example.com",
        "phone":"9876543210",
        "preferences":{
            "language":"English",
            "notifications":true
        }
    }'
);


CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    OrderDetails JSON
);

INSERT INTO Orders
VALUES
(
    1001,
    'Alice',
    '{
        "items":[
            {"product":"Laptop","qty":1,"price":75000},
            {"product":"Mouse","qty":2,"price":500}
        ],
        "shipping":{
            "city":"Pune",
            "state":"Maharashtra"
        }
    }'
);
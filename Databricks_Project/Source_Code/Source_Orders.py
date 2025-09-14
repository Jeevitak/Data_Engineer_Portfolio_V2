create table orders
(
  order_id INT,
  order_date DATE,
  cutomer_id INT,
  order_status STRING
);
Insert into orders 
values
(1, '2022-01-01', 1, 'completed'),
(2, '2022-01-02', 2, 'pending'),
(3, '2022-01-03', 3, 'completed'),
(4, '2022-01-04', 4, 'completed'),
(5, '2022-01-05', 5, 'cancelled');
Insert into orders
values
(6, '2022-01-06', 6, 'completed'),
(7, '2022-01-07', 7,'Pending');

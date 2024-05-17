--Question1
select customer_id
from customer_sales
group by customer_id
having count(*) > 1;


--Question2 
select department_name,MAX(salary)
from employee_tbl e
inner join department_tbl d on e.department_id = d.department_id
group by d.department_name
 

--Question3
select  DATE_FORMAT(order_date, '%Y-%m') as month,
sum(order_total) as total_revenue
from  orders_tbl
group by DATE_FORMAT(order_date, '%Y-%m');
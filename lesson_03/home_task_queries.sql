/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/


select 
	c."name" 
	,count(distinct fc.film_id ) as count_film
from category c 
left join film_category fc  on  c.category_id=fc.category_id --left щоб вивести також ті категорії, у яких немає фільмів
group by c."name" 
order by count_film desc;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/


select 
	a.first_name 
	,a.last_name 
from (--рахуємо по кожному актору к-сть прокатів
	select 
		fa.actor_id
		,count(distinct r.rental_id ) cnt_rental
	from rental r
	join inventory i on r.inventory_id =i.inventory_id 
	join film f on i.film_id=f.film_id 
	join film_actor fa on f.film_id = fa.film_id 
	group by fa.actor_id) rent
join actor a on a.actor_id =rent.actor_id --підтягуємо ім'я
order by cnt_rental desc
limit 10;


/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

--дивимось через вьюху
select category
from sales_by_film_category
order by total_sales desc
limit 1;


----роблю перевірку самостійно

--унікальні платежі (Якщо задублюються дані)
with uniq_pay as (
	select distinct 
		p.payment_id
		,p.amount
		,c."name" 
	from payment p
	join rental r on p.rental_id=r.rental_id 
	join inventory i on r.inventory_id =i.inventory_id 
	join film f on i.film_id=f.film_id 
	join film_category fc on f.film_id=fc.film_id 
	join category c on fc.category_id=c.category_id 
	),
--загальна сума по кожній категорії	
total_amount as (
	select 
		"name"
		,sum(amount) as total_amount
	from uniq_pay
	group by "name"
	),
--проставляємо номери , у найбільшої суми буде номер 1	
runk_category as ( 
	select 
		"name"
		,row_number() over(order by total_amount desc) as num
	from total_amount 
	)
select "name"
from runk_category
where num=1;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

select f.title 
from film f 
left join inventory i on f.film_id =i.film_id 
where i.inventory_id is null;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/


select 
	a.first_name
	,a.last_name 
from  (
	select 
		fa.actor_id
		,count(distinct f.film_id) as count_film
	from film f 
	join film_category fc on f.film_id=fc.category_id 
	join category c on c.category_id=fc.category_id and c."name" ='Children'
	join film_actor fa on f.film_id=fa.film_id 
	group by fa.actor_id ) cnt
join actor a on a.actor_id =cnt.actor_id 
order by cnt.count_film desc
limit 3;

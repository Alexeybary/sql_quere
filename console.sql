--1
select name, count(film_id) as count from film_category
inner join category on film_category.category_id = category.category_id
group by name
order by count desc;
--2
select concat(first_name,' ',last_name) as actor_name, count(rental_id) as count from actor inner join film_actor fa on actor.actor_id = fa.actor_id
    inner join film a on fa.film_id = a.film_id
    inner join inventory i on a.film_id = i.film_id
    inner join rental r on i.inventory_id = r.inventory_id
    group by actor_name
    order by count desc
    limit 10;
;
--3
select  name ,sum(amount) as sum from film_category inner join category c on c.category_id = film_category.category_id
inner join film f on film_category.film_id = f.film_id
inner join inventory i on f.film_id = i.film_id
inner join rental r on i.inventory_id = r.inventory_id
inner join payment p on r.rental_id = p.rental_id
group by name
order by sum desc
limit 1;
--4
select title, film.film_id from film left join inventory i on film.film_id = i.film_id
where inventory_id is null;
--5
with acm as (select concat(first_name,' ',last_name) as  actor_name, count(name) as count from actor
inner join film_actor fa on actor.actor_id = fa.actor_id
inner join film_category fc on fa.film_id = fc.film_id
inner join category c on c.category_id = fc.category_id
where name ='Children'
group by actor_name
order by count desc)
select actor_name, count  from acm
where count in (select count from acm limit 3);
--6
select city.city,count(active) FILTER (WHERE active = 1) AS active_people, count(active) FILTER (WHERE active = 0) AS not_active
from city inner join address a on city.city_id = a.city_id
inner join customer c on a.address_id = c.address_id
group by city.city
order by not_active desc ;
--7
with acm as (select (return_date-rental_date) as rent,title,city.city,name from city inner join address a on city.city_id = a.city_id
inner join customer c on a.address_id = c.address_id
inner join rental r on c.customer_id = r.customer_id
inner join inventory i on i.inventory_id = r.inventory_id
inner join film f on f.film_id = i.film_id
inner join film_category fc on i.film_id = fc.film_id
inner join category c2 on c2.category_id = fc.category_id)
(select sum(rent) as sum_rent, name from acm where  title like 'A%'
group by name
order by sum_rent desc
limit 1)
union
(select sum(rent) as sum_rent, name from acm
where city like '%-%'
group by name
order by sum_rent desc
limit 1);





--UDF : This is the function for sentiment analysis.

CREATE OR REPLACE FUNCTION analyze_sentiment(text STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('textblob') 
HANDLER = 'sentiment_analyzer'
AS $$
from textblob import TextBlob
def sentiment_analyzer(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'
$$;


--yelp reviews table 

--yelp reviews table 

Create or replace table yelp_reviews (review_text variant)
COPY INTO yelp_reviews
FROM 's3://yelpsqlproject/split_files/'
CREDENTIALS = (
    AWS_KEY_ID = '****D7***********'
    AWS_SECRET_KEY = '**********d2t*****************'
FILE_FORMAT = (TYPE = JSON);

create or replace table tbl_yelp_reviews as 
select  review_text:business_id::string as business_id 
,review_text:date::date as review_date 
,review_text:user_id::string as user_id
,review_text:stars::number as review_stars
,review_text:text::string as review_text
,analyze_sentiment(review_text) as sentiments
from yelp_reviews

----yelp_business

Create or replace table yelp_business (business_text variant)

COPY INTO yelp_business
FROM 's3://yelpsqlproject/yelp_academic_dataset_business.json'
CREDENTIALS = (
    AWS_KEY_ID = '****D7***********'
    AWS_SECRET_KEY = '**********d2t*****************'
)
FILE_FORMAT = (TYPE = JSON);

create or replace table tbl_yelp_business as 
select business_text:business_id::string as business_id
,business_text:name::string as name
,business_text:city::string as city
,business_text:state::string as state
,business_text:review_count::string as review_count
,business_text:stars::number as stars
,business_text:categories::string as categories
from yelp_business;


# Ad-hoc analysis of business questions required by stakeholders.

-- Q1: Find the number of businesses in each category.
with cte as (
Select business_id, trim(A.value) as category 
from tbl_yelp_business,
lateral split_to_table (categories, ',') A)

select category, count(*) as no_of_business
from cte
group by 1 order by 2 desc;





-- Q2: Find the top 10 users who have reviewed the most businesses in the " Restaurants" category.

select r.user_id, count(distinct r.business_id) as no_of_business_reviewed 
from tbl_yelp_reviews r 
inner join tbl_yelp_business b 
on r.business_id = b.business_id
where b.categories ilike '%restaurant%'
group by 1 order by 2 desc limit 10;




-- Q3: Find the most popular categories of business (based on the maximum number of reviews).

with cte as (
Select business_id, trim(A.value) as category 
from tbl_yelp_business,
lateral split_to_table (categories, ',') A)

select category, count(*) as no_of_reviews
from cte inner join tbl_yelp_reviews r 
on cte.business_id = r.business_id
group by 1 order by 2 desc;




-- Q4: Find the top 3 most recent reviews for each business.

with cte as (select r.*, b.name, row_number() over(partition by b.business_id order by r.review_date desc) as rn
from tbl_yelp_reviews r 
inner join tbl_yelp_business b 
on r.business_id = b.business_id)

select * from cte where rn<=3;


-- Q5: Find the month with the highest number of reviews.

select TO_CHAR(review_date, 'Mon') as review_month, count(*) as no_of_reviews
from tbl_yelp_reviews
group by 1 order by 2 desc;




-- Q6: Find the percentage of 5-star reviews for each business.


select b.business_id, b.name, count(*) as total_reviews,
sum(case when r.review_stars = 5 then 1 else 0 end) as star5_reviews,
((star5_reviews/total_reviews)*100) as star5_percentage
from tbl_yelp_reviews r 
inner join tbl_yelp_business b 
on r.business_id = b.business_id
group by 1,2;




--Q7: Find the top 5 most reviewed businesses in each city.

with cte as ( 
select b.city, b.business_id, b.name, count(*) as total_reviews,
from tbl_yelp_reviews r 
inner join tbl_yelp_business b 
on r.business_id = b.business_id
group by 1,2,3)

select * from cte
qualify row_number() over(partition by city order by total_reviews desc) <=5;


--Q8: Find the average rating of businesses with at least 100 reviews.

select b.business_id, b.name, count(*) as total_reviews,
avg(r.review_stars) as avg_rating
from tbl_yelp_reviews r 
inner join tbl_yelp_business b 
on r.business_id = b.business_id
group by 1,2 having total_reviews >=100;


--Q9: List the top 10 users who have written the most reviews, along with the businesses they have reviewed.

with cte as (select r.user_id, count(*) as total_reviews,
from tbl_yelp_reviews r 
inner join tbl_yelp_business b 
on r.business_id = b.business_id
group by 1 order by 2 desc limit 10)

select user_id, business_id from tbl_yelp_reviews 
where user_id in (select user_id from cte)
group by 1,2
order by 1 desc;



--Q10: Find the top 10 businesses with the highest positive sentiment reviews.

select r.business_id, b.name, count(*) as total_reviews,
from tbl_yelp_reviews r 
inner join tbl_yelp_business b 
on r.business_id = b.business_id
where sentiments = 'Positive'
group by 1,2 order by 3 desc limit 10;




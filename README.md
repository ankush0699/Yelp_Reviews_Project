# Yelp_Reviews_Project
This repo has my project for doing sentimental/ad-hoc analysis of yelp reviews. In this project I have used Python, AWS, Snowflake and SQL.

This project is focusing on review dataset from yelp to do sentimental analysis (SQL code is given in file) and ad-hoc analysis of reviews by fetching data using SQL query in Snowflake.

Here is the link for datset: https://business.yelp.com/external-assets/files/Yelp-JSON.zip

Be aware of the fact that this is a very hige file and uploading it directly in snowflake or any other DBMS system will make query run slower.

Hence I have written a python code to split required file into very small files and then I have created an S3 buscket on AWS for seemless integration of datset into snoflake. 

All sql codes are written in snowflake.

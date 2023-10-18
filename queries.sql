--QUESTION 1

--How many animals of each type have outcomes?
--I.e. how many cats, dogs, birds etc. Note that this question is asking about number of animals, 
--not number of outcomes, so animals with multiple outcomes should be counted only once.


SELECT animal_type, count(*) FROM animal GROUP BY animal_type;


--QUESTION 2

--How many animals are there with more than 1 outcome?

SELECT
    COUNT(animal_id) AS "Animals with More than 1 Outcome"
FROM (
    SELECT
        animal_id
    FROM
        outcome_events
    GROUP BY
        animal_id
    HAVING
        COUNT(*) > 1
) AS Subquery;

--QUESTION 3

--What are the top 5 months for outcomes? 
--Calendar months in general, not months of a particular year. This means answer will be like April, October, etc rather than April 2013, October 2018, 


SELECT
    CASE
        WHEN extracted_month = 1 THEN 'January'
        WHEN extracted_month = 2 THEN 'February'
        WHEN extracted_month = 3 THEN 'March'
        WHEN extracted_month = 4 THEN 'April'
        WHEN extracted_month = 5 THEN 'May'
        WHEN extracted_month = 6 THEN 'June'
        WHEN extracted_month = 7 THEN 'July'
        WHEN extracted_month = 8 THEN 'August'
        WHEN extracted_month = 9 THEN 'September'
        WHEN extracted_month = 10 THEN 'October'
        WHEN extracted_month = 11 THEN 'November'
        WHEN extracted_month = 12 THEN 'December'
        ELSE 'Invalid Month'
    END AS month_name,
    COUNT(*) AS month_count
FROM (SELECT EXTRACT(MONTH FROM datetime) AS extracted_month FROM outcome_events) subquery
GROUP BY extracted_month
ORDER BY month_count DESC
LIMIT 5;


--QUESTION 4

-- A "Kitten" is a "Cat" who is less than 1 year old. A "Senior cat" is a "Cat" who is over 10 years old. An "Adult" is a cat who is between 1 and 10 years old.
-- What is the total number of kittens, adults, and seniors, whose outcome is "Adopted"?
-- Conversely, among all the cats who were "Adopted", what is the total number of kittens, adults, and seniors?


select age_category , count(*) from (SELECT
    age_in_years,
    outcome_type,
    CASE
        WHEN age_in_years < 1 THEN 'Kitten'
        WHEN age_in_years > 10 THEN 'Senior Cat'
        ELSE 'Adult'
    END AS age_category
FROM (SELECT
    a.animal_type,
    DATE_PART('year', AGE(o.datetime, a.date_of_birth)) AS age_in_years,
    ot.outcome_type 
FROM
    animal AS a
JOIN
    outcome_events AS o ON a.animal_id = o.animal_id
JOIN
    outcome_type AS ot ON ot.outcome_type_id = o.outcome_type_id where outcome_type = 'Adoption' and animal_type = 'Cat')) group by age_category;

--QUESTION 5

--For each date, what is the cumulative total of outcomes up to and including this date?
--Note: this type of question is usually used to create dashboard for progression of quarterly metrics. 
--In SQL, this is usually accomplished using something called Window Functions. You'll need to research and learn this on your own!

   
SELECT
    date(datetime) AS "Date",
    COUNT(*) AS "Daily Outcomes",
    SUM(COUNT(*)) OVER (ORDER BY date(datetime)) AS "Cumulative Total Outcomes"
FROM outcome_events
GROUP BY date(datetime)
ORDER BY date(datetime);

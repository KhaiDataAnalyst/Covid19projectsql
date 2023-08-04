-- COVID19 DATA EXPLORATION 
-- Skills that I used: Aggregate function, Converting data type, Window Functions like: ROW_NUMBER,PARTITION BY,...,CREATE VIEW,CTE's,Joins,Temp Tables,Use Cases

--CovidDeaths datasets
SELECT * FROM CovidDeaths
WHERE continent is not null

--CovidVaccinations datasets
SELECT  * FROM CovidVaccinations
WHERE continent is not null 

-- The number of rows of the dataset
SELECT COUNT(*) FROM CovidDeaths

-- Years when we do the survey
-- From the result, we can see that this survey is conducted in two years 2020 and 2021
SELECT DISTINCT(YEAR(date)) AS Year FROM CovidDeaths
SELECT DISTINCT(YEAR(date)) AS Year FROM CovidVaccinations

--The global number 
SELECT location,max(total_cases) as total_cases,max(total_deaths) as total_death
FROM CovidDeaths
WHERE continent is null
GROUP BY location
--As we can observe, the rows where continent is null show the figures of each continent and the world
 
-- Find out the number of vaccinated and unvaccinated people in the world
SELECT location,max(people_vaccinated) as people_vaccinated,max(population)-max(people_vaccinated) as people_unvaccinated
FROM  CovidDeaths
WHERE location='World'
GROUP BY location

-- Find out the death_rate of each continent over the period
SELECT continent,sum(new_cases) as total_cases,sum(new_deaths) as total_deaths
--cast(sum(new_deaths) as float)/cast(sum(new_cases) as float)*100 as death_rate
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent


--Continue with location
--Find out the gdp of each location in the latest year in the dataset
SELECT distinct(location),gdp_per_capita,continent,
CASE	
	WHEN gdp_per_capita<=1025 THEN 'Low Income'
	WHEN gdp_per_capita<=3995 THEN 'Lower-middle Income'
	WHEN gdp_per_capita<=12375 THEN 'Upper-middle Income'
	WHEN gdp_per_capita>12375 THEN 'High Income'
	ELSE 'Unknown'
END AS Income_groups
FROM CovidDeaths
where continent is not null 
ORDER BY gdp_per_capita desc

--Find  out the Dthe number death of each location over the period of time 
SELECT location,date,sum(new_deaths) over(partition by date) AS number_of_deaths
FROM CovidDeaths
WHERE continent is not NULL
OR BY 2

-- Find out the top 10 with the highest total deaths in 2020
SELECT top 10 location,MAX(total_deaths) as TotalDeaths FROM CovidDeaths 
WHERE continent is not null AND YEAR(date)=2020
GROUP BY location
ORDER BY TotalDeaths DESC

-- Find out the top 10 countries with the highest total deaths in 2021
SELECT top 10 location,MAX(total_deaths) as TotalDeaths FROM CovidDeaths 
WHERE continent is not null AND YEAR(date)=2021
GROUP BY location
ORDER BY TotalDeaths DESC

--Union two years
SELECT location, TotalDeaths, Year
FROM (
    SELECT location, MAX(total_deaths) as TotalDeaths, YEAR(date) as Year,
           ROW_NUMBER() OVER (ORDER BY MAX(total_deaths) DESC) as rn
    FROM CovidDeaths
    WHERE continent IS NOT NULL AND YEAR(date) = 2020
    GROUP BY location, YEAR(date)
) AS temp
WHERE rn <= 10
UNION ALL
SELECT location, TotalDeaths, Year
FROM (
    SELECT location, MAX(total_deaths) as TotalDeaths, YEAR(date) as Year,
           ROW_NUMBER() OVER (ORDER BY MAX(total_deaths) DESC) as rn
    FROM CovidDeaths
    WHERE continent IS NOT NULL AND YEAR(date) = 2021
    GROUP BY location, YEAR(date)
) AS temp2
WHERE rn <= 10
ORDER BY [Year]



-- Break down the death_rate in each location
SELECT location,max(total_deaths) as total_death,max(total_cases) as total_cases,
(cast(max(total_deaths) as float)/cast(max(total_cases) as float))*100 as death_rate
FROM CovidDeaths
WHERE continent is not null 
GROUP BY location
ORDER BY death_rate

-- Countries with Highest Infection Rate compared to Population
SELECT location,population,round(cast(max(total_cases) as float)/cast(population as float)*100,2) as infection_rate
FROM CovidDeaths
WHERE continent is not null
GROUP BY location,population 
ORDER BY infection_rate desc


-- Find out the country that has the highest total_vaccination in each continent
SELECT location,continent,total_vaccinations FROM(
SELECT location,continent,total_vaccinations,ROW_NUMBER() OVER (PARTITION BY continent ORDER BY total_vaccinationS DESC) position
FROM CovidVaccinations
WHERE continent IS NOT NULL) total_vac
WHERE position=1




-- Join the two table, extract necessary information
SELECT death.continent, death.location,death.population,
sum(death.new_cases) over(partition by death.location order by death.date) as total_cases,
max(death.people_fully_vaccinated) over (partition by vaccine.location) as total_fully_vaccinated,
ROW_NUMBER() over (partition by death.location order by death.total_cases)
FROM CovidDeaths AS death
JOIN CovidVaccinations AS vaccine
ON death.location=vaccine.location
AND death.date=vaccine.date
WHERE death.continent is not null 

--Create View for later visualization, show the data for each country
Create View fully_vaccinated  As
With CTE as(
SELECT death.continent, death.location,death.population,
max(death.people_fully_vaccinated) over (partition by vaccine.location) as total_fully_vaccinated,
ROW_NUMBER() over (partition by vaccine.location order by vaccine.people_fully_vaccinated desc) as position
FROM CovidDeaths AS death
JOIN CovidVaccinations AS vaccine
ON death.location=vaccine.location AND death.date=vaccine.date
WHERE death.continent is not null )
Select * From CTE
WHERE position=1
select * from fully_vaccinated

--Create a new table name vaccination
CREATE TABLE vaccination (
Continent nvarchar(50) not null,
Country nvarchar(50) unique not null,
Population bigint,
total_fully_vaccinated bigint)
 
 --Insert necessary data into the table vaccination
INSERT INTO vaccination
SELECT continent,location,population,total_fully_vaccinated
FROM fully_vaccinated
WHERE total_fully_vaccinated IS NOT NULL
-- Delete the table
DROP TABLE IF EXISTS vaccination

Select * from CovidDeaths



























  





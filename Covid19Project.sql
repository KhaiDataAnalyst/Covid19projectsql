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
SELECT DISTINCT(YEAR(date)) AS Year FROM CovidDeaths
SELECT DISTINCT(YEAR(date)) AS Year FROM CovidVaccinations
-- From the result, we can see that this survey is conducted in two years 2020 and 2021

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

-- Find out the death_rate of each continent
SELECT continent,sum(new_cases) as total_cases,sum(new_deaths) as total_deaths,
ROUND(cast(sum(new_deaths) as float)/nullif(cast(sum(new_cases) as float)*100,0),5) as death_rate
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

--Find out the number death of each location over the period of time 
SELECT location,date,sum(new_deaths) over(partition by date) AS number_of_deaths
FROM CovidDeaths
WHERE continent is not NULL
ORDER BY date

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

--Union two years above
SELECT location,total_deaths,year from(
SELECT top 10 location,max(total_deaths) as total_deaths,year(date) as year from CovidDeaths
WHERE continent is not null and year(date)=2020
GROUP BY location,year(date) 
ORDER BY max(total_deaths) DESC) as table1
UNION ALL
SELECT location,total_deaths,year from(
SELECT top 10 location,max(total_deaths) as total_deaths,year(date) as year from CovidDeaths
WHERE continent is not null and year(date)=2021
GROUP BY location,year(date)
ORDER BY max(total_deaths) DESC) as table2
ORDER BY year
 
-- Break down the death_rate and vaccination_rate in each location
SELECT dea.location,ROUND(cast(max(dea.total_deaths) as float)/nullif(cast(max(dea.total_cases) as float),0),5) as death_rate,
round(cast(max(vac.people_vaccinated) as float)/nullif(cast(max(dea.population) as float),0),5) as vaccination_rate
FROM CovidDeaths as dea
JOIN CovidVaccinations as vac
ON dea.iso_code=vac.iso_code AND dea.date=vac.date
WHERE dea.continent is not null
GROUP BY dea.location

-- Countries with Highest Infection Rate compared to Population
SELECT location,population,round(cast(max(total_cases) as float)/nullif(cast(population as float),0)*100,5) as infection_rate
FROM CovidDeaths
WHERE continent is not null
GROUP BY location,population 
ORDER BY infection_rate desc


-- Find out the country that has the highest total_vaccination in each continent
-- Use row_number() when you want to find out the highest/lowest component in each divided group
SELECT location,continent,total_vaccinations FROM(
SELECT location,continent,total_vaccinations,ROW_NUMBER() OVER (PARTITION BY continent ORDER BY total_vaccinations DESC) as position
FROM CovidVaccinations
WHERE continent IS NOT NULL) as total_vac
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
Create View fully_vaccinated As
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




























  





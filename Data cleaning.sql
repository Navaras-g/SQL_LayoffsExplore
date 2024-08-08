select *
from layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any unnecessary columns

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as ROW_NUM
FROM layoffs_staging;

WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as ROW_NUM
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE ROW_NUM > 1;

SELECT *
FROM layoffs_staging
WHERE company =  'Casper';


WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as ROW_NUM
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE ROW_NUM > 1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as ROW_NUM
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y')
;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''; 

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
    AND T1.location = T2.location
WHERE (T1.industry IS NULL OR T1.industry = '')
AND T2.industry IS NOT NULL;

SELECT T1.industry, T2.industry
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
    AND T1.location = T2.location
WHERE (T1.industry IS NULL OR T1.industry = '')
AND T2.industry IS NOT NULL;


UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
    SET T1.industry = T2.industry
WHERE (T1.industry IS NULL)
AND T2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- DATA HAS BEEN CLEANED.










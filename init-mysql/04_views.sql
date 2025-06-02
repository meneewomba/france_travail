USE mydb;

DROP VIEW IF EXISTS salary_per_dept;
create view   salary_per_dept AS (
	   select count(0) AS `COUNT(*)`,
left(`a`.`insee_code`,2) AS `dept`,
(case 
when ((`b`.`max_monthly_salary` between 0 and 1000) or (`b`.`max_monthly_predicted` between 0 and 1000 AND b.avg_max_diff BETWEEN -10 AND 10)) then '0-1000' 
when ((`b`.`max_monthly_salary` between 1001 and 2000) or (`b`.`max_monthly_predicted` between 1001 and 2000 AND b.avg_max_diff BETWEEN -10 AND 10)) then '1000-2000' 
when ((`b`.`max_monthly_salary` between 2001 and 3000) or (`b`.`max_monthly_predicted` between 2001 and 3000 AND b.avg_max_diff BETWEEN -10 AND 10)) then '2000-3000' 
when ((`b`.`max_monthly_salary` between 3001 and 4000) or (`b`.`max_monthly_predicted` between 3001 and 4000 AND b.avg_max_diff BETWEEN -10 AND 10)) then '3000-4000' 
when ((`b`.`max_monthly_salary` > 4001) or (`b`.`max_monthly_predicted` > 4001 AND b.avg_max_diff BETWEEN -10 AND 10)) then '4000+' end) AS `salary_range` 
from (`job` `a` join `salary` `b` on((`a`.`job_id` = `b`.`job_id`)))
 where ((`b`.`max_monthly_salary` > 0) or (`b`.`max_monthly_predicted` > 0))  group by `dept`,`salary_range` 
 having (`salary_range` is not NULL) AND dept != 62   order by `dept`,`salary_range`
	   );

DROP VIEW IF EXISTS salary_per_dept_no_pred;
create view   salary_per_dept_no_pred AS (
	   select count(0) AS `COUNT(*)`,
left(`a`.`insee_code`,2) AS `dept`,
(case 
when `b`.`max_monthly_salary` between 0 and 1000 then '0-1000' 
when `b`.`max_monthly_salary` between 1001 and 2000 then '1000-2000' 
when `b`.`max_monthly_salary` between 2001 and 3000 then '2000-3000' 
when `b`.`max_monthly_salary` between 3001 and 4000 then '3000-4000' 
when `b`.`max_monthly_salary` > 4001  then '4000+' end) AS `salary_range` 
from (`job` `a` join `salary` `b` on((`a`.`job_id` = `b`.`job_id`)))
 where `b`.`max_monthly_salary` > 0  group by `dept`,`salary_range` 
 having (`salary_range` is not NULL) AND dept != 62   order by `dept`,`salary_range`
	   );

DROP VIEW IF EXISTS most_wanted;
create view   most_wanted AS(
		with cte AS (
		SELECT COUNT(a.job_id) cnt, LEFT(a.insee_code,2) dept , b.label, ROW_NUMBER() OVER(PARTITION BY LEFT(insee_code, 2) 
		ORDER BY COUNT(a.job_id) DESC
		) AS RANK_job
		FROM job a JOIN rome b ON a.rome_code = b.rome_code GROUP BY b.label, dept ORDER BY dept, cnt DESC, rank_job)
		SELECT * FROM cte WHERE rank_job <= 30
		);

DROP VIEW IF EXISTS jobs_per_companies;
create view  jobs_per_companies as(
		with cte AS (
		SELECT COUNT(job_id) cnt, LEFT(insee_code,2) dept, b.name, ROW_NUMBER() OVER (PARTITION BY LEFT(insee_code,2) 
		ORDER BY COUNT(job_id) DESC 
		) AS rank_ent 
		FROM job a JOIN companies b ON a.company_id = b.company_id
		GROUP BY  b.name, dept  
		ORDER BY dept, cnt DESC, rank_ent)
		SELECT * FROM cte WHERE rank_ent <=50);


DROP VIEW IF EXISTS avg_salary_rome;
create view  avg_salary_rome AS (WITH base AS (
				SELECT 
					j.job_id,
					j.rome_code,
					CASE 
						WHEN j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0 THEN 0 
						ELSE 1 
					END AS experience_group,
					s.min_monthly_salary,
					s.max_monthly_salary
				FROM job j
				JOIN salary s ON j.job_id = s.job_id
				WHERE s.min_monthly_salary > 0 AND s.max_monthly_salary > 0
			),

			avg_salary AS (
				SELECT 
					rome_code,
					experience_group,
					ROUND(AVG(min_monthly_salary), 2) AS moy_min,
					ROUND(AVG(max_monthly_salary), 2) AS moy_max
				FROM base
				GROUP BY rome_code, experience_group
			),

			-- Moyenne fallback à 3 caractères
			fallback_3 AS (
				SELECT 
					LEFT(j.rome_code, 3) AS rome3,
					ROUND(AVG(s.min_monthly_salary), 2) AS moy_min_3,
					ROUND(AVG(s.max_monthly_salary), 2) AS moy_max_3
				FROM job j
				JOIN salary s ON j.job_id = s.job_id
				WHERE 
					(j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0)
					AND s.min_monthly_salary > 0 AND s.max_monthly_salary > 0
				GROUP BY LEFT(j.rome_code, 3)
			),

			-- Moyenne fallback à 2 caractères
			fallback_2 AS (
				SELECT 
					LEFT(j.rome_code, 2) AS rome2,
					ROUND(AVG(s.min_monthly_salary), 2) AS moy_min_2,
					ROUND(AVG(s.max_monthly_salary), 2) AS moy_max_2
				FROM job j
				JOIN salary s ON j.job_id = s.job_id
				WHERE 
					(j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0)
					AND s.min_monthly_salary > 0 AND s.max_monthly_salary > 0
				GROUP BY LEFT(j.rome_code, 2)
			),

			-- Moyenne fallback à 4 caractères
			fallback_4 AS (
				SELECT 
					LEFT(j.rome_code, 4) AS rome4,
					ROUND(AVG(s.min_monthly_salary), 2) AS moy_min_4,
					ROUND(AVG(s.max_monthly_salary), 2) AS moy_max_4
				FROM job j
				JOIN salary s ON j.job_id = s.job_id
				WHERE 
					(j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0)
					AND s.min_monthly_salary > 0 AND s.max_monthly_salary > 0
				GROUP BY LEFT(j.rome_code, 4)
			)

			-- Moyenne finale
			SELECT 
				j.job_id,
				j.rome_code,
				COALESCE(a.experience_group, 
						CASE 
							WHEN j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0 THEN 0 
							ELSE 1 
						END) AS experience_group,
						
				-- Choix des moyennes par priorité : directe > fallback_4 > fallback 3 > fallback 2
				COALESCE(a.moy_min, f4.moy_min_4, f3.moy_min_3, f2.moy_min_2) AS moy_min,
				COALESCE(a.moy_max, f4.moy_max_4, f3.moy_max_3, f2.moy_max_2) AS moy_max,

				-- indicateur de fallback (0 = direct, 1 = fallback 3, 2 = fallback 2)
				CASE 
					WHEN a.moy_min IS NOT NULL THEN 0
					WHEN f4.moy_min_4 IS NOT NULL then 1
					WHEN f3.moy_min_3 IS NOT NULL THEN 2
					ELSE 3
				END AS fallback_level

			FROM job j
			LEFT JOIN avg_salary a 
				ON j.rome_code = a.rome_code 
				AND (
					(j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0 AND a.experience_group = 0)
					OR (j.experience_length_months IS NOT NULL AND j.experience_length_months > 0 AND a.experience_group = 1)
				)
			LEFT JOIN fallback_4 f4 ON LEFT(j.rome_code, 4) = f4.rome4
			LEFT JOIN fallback_3 f3 ON LEFT(j.rome_code, 3) = f3.rome3
			LEFT JOIN fallback_2 f2 ON LEFT(j.rome_code, 2) = f2.rome2
			GROUP BY j.job_id, a.rome_code, a.experience_group, 
				a.moy_min, a.moy_max, f4.moy_min_4, f4.moy_max_4, 
				f3.moy_min_3, f3.moy_max_3, f2.moy_min_2, f2.moy_max_2);
			
			
DROP VIEW IF EXISTS missing_datas;
create view  missing_datas AS (with `missing_salaries` AS 
(select count(0) AS `count` from `salary` where (((`salary`.`min_monthly_salary` = 0) or (`salary`.`min_monthly_salary` is null)) and ((`salary`.`max_monthly_salary` = 0) or (`salary`.`max_monthly_salary` is NULL)))),
 `missing_formations` as (select count(0) AS `count` from `job` where `job`.`job_id` in (select `job_formation`.`job_id` from `job_formation`) is FALSE),
  `missing_qualities` as (select count(0) AS `count` from `job` where `job`.`job_id` in (select `job_professional_qualities`.`job_id` from `job_professional_qualities`) is false), 
  `missing_language` as (select count(0) AS `count` from `job` where `job`.`job_id` in (select `job_language`.`job_id` from `job_language`) is false), 
  `missing_competencies` as (select count(0) AS `count` from `job` where `job`.`job_id` in (select `job_competency`.`job_id` from `job_competency`) is false), 
  `missing_benefits` as (select count(0) AS `count` from `job` where `job`.`job_id` in (select `a`.`job_id` from (`salary` `a` join `salary_benefits` `b` on((`a`.`salary_id` = `b`.`salary_id`)))) is false) 
  
  select 'missing_salaries' AS `missing_types`,`missing_salaries`.`count` AS `count` from `missing_salaries` 
  union all 
  select 'missing_formations' AS `missing_formations`,`missing_formations`.`count` AS `count` from `missing_formations` 
  union all
   select 'missing_qualities' AS `missing_qualities`,`missing_qualities`.`count` AS `count` from `missing_qualities`
	 union all
	  select 'missing_language' AS `missing_language`,`missing_language`.`count` AS `count` from `missing_language`
	   union all 
		select 'missing_competencies' AS `missing_competencies`,`missing_competencies`.`count` AS `count` from `missing_competencies` 
		union all 
		select 'missing_benefits' AS `missing_benefits`,`missing_benefits`.`count` AS `count` from `missing_benefits` 
		union all 
		select 'total_jobs' AS `total_jobs`,count(0) AS `count` from `job`
		UNION all
		SELECT 'missing_qualifications' AS missing_qualifications, COUNT(0) AS `count` FROM job WHERE qualification_code IS null
		UNION all
		SELECT 'missing_activitiy_sector' AS missing_activity_sector, COUNT(0) AS `count`FROM job WHERE activity_sector_code IS null
		UNION all
		SELECT 'missing_companies' AS missing_companies, COUNT(0) AS `count`FROM job WHERE company_id IS null
		);

use ft_test;
DROP VIEW IF EXISTS avg_salary_rome;
create view  avg_salary_rome AS (WITH base AS (
				SELECT 
					j.job_id,
					j.rome_code,
					CASE 
						WHEN j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0 THEN 0 
						ELSE 1 
					END AS experience_group,
					s.min_monthly_salary,
					s.max_monthly_salary
				FROM job j
				JOIN salary s ON j.job_id = s.job_id
				WHERE s.min_monthly_salary > 0 AND s.max_monthly_salary > 0
			),

			avg_salary AS (
				SELECT 
					rome_code,
					experience_group,
					ROUND(AVG(min_monthly_salary), 2) AS moy_min,
					ROUND(AVG(max_monthly_salary), 2) AS moy_max
				FROM base
				GROUP BY rome_code, experience_group
			),

			-- Moyenne fallback à 3 caractères
			fallback_3 AS (
				SELECT 
					LEFT(j.rome_code, 3) AS rome3,
					ROUND(AVG(s.min_monthly_salary), 2) AS moy_min_3,
					ROUND(AVG(s.max_monthly_salary), 2) AS moy_max_3
				FROM job j
				JOIN salary s ON j.job_id = s.job_id
				WHERE 
					(j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0)
					AND s.min_monthly_salary > 0 AND s.max_monthly_salary > 0
				GROUP BY LEFT(j.rome_code, 3)
			),

			-- Moyenne fallback à 2 caractères
			fallback_2 AS (
				SELECT 
					LEFT(j.rome_code, 2) AS rome2,
					ROUND(AVG(s.min_monthly_salary), 2) AS moy_min_2,
					ROUND(AVG(s.max_monthly_salary), 2) AS moy_max_2
				FROM job j
				JOIN salary s ON j.job_id = s.job_id
				WHERE 
					(j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0)
					AND s.min_monthly_salary > 0 AND s.max_monthly_salary > 0
				GROUP BY LEFT(j.rome_code, 2)
			),

			-- Moyenne fallback à 4 caractères
			fallback_4 AS (
				SELECT 
					LEFT(j.rome_code, 4) AS rome4,
					ROUND(AVG(s.min_monthly_salary), 2) AS moy_min_4,
					ROUND(AVG(s.max_monthly_salary), 2) AS moy_max_4
				FROM job j
				JOIN salary s ON j.job_id = s.job_id
				WHERE 
					(j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0)
					AND s.min_monthly_salary > 0 AND s.max_monthly_salary > 0
				GROUP BY LEFT(j.rome_code, 4)
			)

			-- Moyenne finale
			SELECT 
				j.job_id,
				j.rome_code,
				COALESCE(a.experience_group, 
						CASE 
							WHEN j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0 THEN 0 
							ELSE 1 
						END) AS experience_group,
						
				-- Choix des moyennes par priorité : directe > fallback_4 > fallback 3 > fallback 2
				COALESCE(a.moy_min, f4.moy_min_4, f3.moy_min_3, f2.moy_min_2) AS moy_min,
				COALESCE(a.moy_max, f4.moy_max_4, f3.moy_max_3, f2.moy_max_2) AS moy_max,

				-- indicateur de fallback (0 = direct, 1 = fallback 3, 2 = fallback 2)
				CASE 
					WHEN a.moy_min IS NOT NULL THEN 0
					WHEN f4.moy_min_4 IS NOT NULL then 1
					WHEN f3.moy_min_3 IS NOT NULL THEN 2
					ELSE 3
				END AS fallback_level

			FROM job j
			LEFT JOIN avg_salary a 
				ON j.rome_code = a.rome_code 
				AND (
					(j.experience_required = 'D' OR j.experience_length_months IS NULL OR j.experience_length_months = 0 AND a.experience_group = 0)
					OR (j.experience_length_months IS NOT NULL AND j.experience_length_months > 0 AND a.experience_group = 1)
				)
			LEFT JOIN fallback_4 f4 ON LEFT(j.rome_code, 4) = f4.rome4
			LEFT JOIN fallback_3 f3 ON LEFT(j.rome_code, 3) = f3.rome3
			LEFT JOIN fallback_2 f2 ON LEFT(j.rome_code, 2) = f2.rome2
			GROUP BY j.job_id, a.rome_code, a.experience_group, 
				a.moy_min, a.moy_max, f4.moy_min_4, f4.moy_max_4, 
				f3.moy_min_3, f3.moy_max_3, f2.moy_min_2, f2.moy_max_2);

